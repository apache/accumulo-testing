/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.testing.continuous;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.primitives.UnsignedBytes;

public class BulkBatchWriter implements BatchWriter {

  private static final Logger log = LoggerFactory.getLogger(BulkBatchWriter.class);

  private final List<Mutation> mutations = new ArrayList<>();
  private final AccumuloClient client;
  private final String tableName;
  private final FileSystem fileSystem;
  private final Path workPath;
  private final long memLimit;
  private final Supplier<SortedSet<Text>> splitSupplier;

  private long memUsed;
  private boolean closed = false;

  public BulkBatchWriter(AccumuloClient client, String tableName, FileSystem fileSystem,
      Path workPath, long memLimit, Supplier<SortedSet<Text>> splitSupplier) {
    this.client = client;
    this.tableName = tableName;
    this.fileSystem = fileSystem;
    this.workPath = workPath;
    this.memLimit = memLimit;
    this.splitSupplier = splitSupplier;
  }

  @Override
  public synchronized void addMutation(Mutation mutation) throws MutationsRejectedException {
    Preconditions.checkState(!closed);
    mutation = new Mutation(mutation);
    mutations.add(mutation);
    memUsed += mutation.estimatedMemoryUsed();
    if (memUsed > memLimit) {
      flush();
    }
  }

  @Override
  public synchronized void addMutations(Iterable<Mutation> iterable)
      throws MutationsRejectedException {
    for (var mutation : iterable) {
      addMutation(mutation);
    }
  }

  @Override
  public synchronized void flush() throws MutationsRejectedException {
    Preconditions.checkState(!closed);

    try {
      var splits = splitSupplier.get();

      var comparator = UnsignedBytes.lexicographicalComparator();

      Path tmpDir = new Path(workPath, UUID.randomUUID().toString());
      fileSystem.mkdirs(tmpDir);
      mutations.sort((m1, m2) -> comparator.compare(m1.getRow(), m2.getRow()));

      RFileWriter writer = null;
      byte[] currEndRow = null;
      int nextFileNameCounter = 0;

      var loadPlanBuilder = LoadPlan.builder();

      for (var mutation : mutations) {
        if (writer == null
            || (currEndRow != null && comparator.compare(mutation.getRow(), currEndRow) > 0)) {
          if (writer != null) {
            writer.close();
          }

          var row = new Text(mutation.getRow());
          var headSet = splits.headSet(row);
          var tabletPrevRow = headSet.isEmpty() ? null : headSet.last();
          var tailSet = splits.tailSet(row);
          var tabletEndRow = tailSet.isEmpty() ? null : tailSet.first();
          currEndRow = tabletEndRow == null ? null : tabletEndRow.copyBytes();

          String filename = String.format("bbw-%05d.rf", nextFileNameCounter++);
          writer = RFile.newWriter().to(tmpDir + "/" + filename).withFileSystem(fileSystem).build();
          loadPlanBuilder = loadPlanBuilder.loadFileTo(filename, LoadPlan.RangeType.TABLE,
              tabletPrevRow, tabletEndRow);

          log.debug("Created new file {} for range {} {}", filename, tabletPrevRow, tabletEndRow);
        }

        for (var colUpdate : mutation.getUpdates()) {
          var key = new Key(mutation.getRow(), colUpdate.getColumnFamily(),
              colUpdate.getColumnQualifier(), colUpdate.getColumnVisibility());
          if (colUpdate.hasTimestamp()) {
            key.setTimestamp(colUpdate.getTimestamp());
          }
          if (colUpdate.isDeleted()) {
            key.setDeleted(true);
          }
          writer.append(key, colUpdate.getValue());
        }
      }

      if (writer != null) {
        writer.close();
      }

      // TODO make table time configurable?
      var loadPlan = loadPlanBuilder.build();

      long t1 = System.nanoTime();
      client.tableOperations().importDirectory(tmpDir.toString()).to(tableName).plan(loadPlan)
          .tableTime(true).load();
      long t2 = System.nanoTime();

      log.debug("Bulk imported dir {} destinations:{} mutations:{} memUsed:{} time:{}ms", tmpDir,
          loadPlan.getDestinations().size(), mutations.size(), memUsed,
          TimeUnit.NANOSECONDS.toMillis(t2 - t1));

      fileSystem.delete(tmpDir, true);

      mutations.clear();
      memUsed = 0;
    } catch (Exception e) {
      closed = true;
      throw new MutationsRejectedException(client, List.of(), Map.of(), List.of(), 1, e);
    }
  }

  @Override
  public synchronized void close() throws MutationsRejectedException {
    flush();
    closed = true;
  }
}
