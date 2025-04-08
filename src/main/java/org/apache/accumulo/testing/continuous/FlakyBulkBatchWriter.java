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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
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
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

/**
 * BatchWriter that bulk imports in its implementation. The implementation contains a bug that was
 * found to be useful for testing Accumulo. The bug was left and this class was renamed to add Flaky
 * to indicate its danger for other uses.
 */
public class FlakyBulkBatchWriter implements BatchWriter {

  private static final Logger log = LoggerFactory.getLogger(FlakyBulkBatchWriter.class);

  private final Deque<Mutation> mutations = new ArrayDeque<>();
  private final AccumuloClient client;
  private final String tableName;
  private final FileSystem fileSystem;
  private final Path workPath;
  private final long memLimit;
  private final Supplier<SortedSet<Text>> splitSupplier;

  private long memUsed;
  private boolean closed = false;

  public FlakyBulkBatchWriter(AccumuloClient client, String tableName, FileSystem fileSystem,
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
    mutations.addLast(mutation);
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

      Path tmpDir = new Path(workPath, UUID.randomUUID().toString());
      fileSystem.mkdirs(tmpDir);

      List<KeyValue> keysValues = new ArrayList<>(mutations.size());

      // remove mutations from the dequeue as we convert them to Keys making the Mutation objects
      // available for garbage collection
      Mutation mutation;
      while ((mutation = mutations.pollFirst()) != null) {
        for (var columnUpdate : mutation.getUpdates()) {
          var builder = Key.builder(false).row(mutation.getRow())
              .family(columnUpdate.getColumnFamily()).qualifier(columnUpdate.getColumnQualifier())
              .visibility(columnUpdate.getColumnVisibility());
          if (columnUpdate.hasTimestamp()) {
            builder = builder.timestamp(columnUpdate.getTimestamp());
          }
          Key key = builder.deleted(columnUpdate.isDeleted()).build();
          keysValues.add(new KeyValue(key, columnUpdate.getValue()));
        }
      }

      keysValues.sort(Map.Entry.comparingByKey());

      RFileWriter writer = null;
      byte[] currEndRow = null;
      int nextFileNameCounter = 0;

      var loadPlanBuilder = LoadPlan.builder();

      // This code is broken because Arrays.compare will compare bytes as signed integers. Accumulo
      // treats bytes as unsigned 8 bit integers for sorting purposes. This incorrect comparator
      // causes this code to sometimes prematurely close rfiles, which can lead to lots of files
      // being bulk imported into a single tablet. The files still go to the correct tablet, so this
      // does not cause data loss. This bug was found to be useful in testing as it introduces
      // stress on bulk import+compactions and it was decided to keep this bug. If copying this code
      // elsewhere then this bug should probably be fixed.
      Comparator<byte[]> comparator = Arrays::compare;
      // To fix the code above it should be replaced with the following
      // Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

      for (var keyValue : keysValues) {
        var key = keyValue.getKey();
        if (writer == null || (currEndRow != null
            && comparator.compare(key.getRowData().toArray(), currEndRow) > 0)) {
          if (writer != null) {
            writer.close();
          }

          // When the above code prematurely closes a rfile because of the incorrect comparator, the
          // following code will find a new Tablet. Since the following code uses the Text
          // comparator its comparisons are correct and it will just find the same tablet for the
          // file that was just closed. This is what cause multiple files to added to the same
          // tablet.
          var row = key.getRow();
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

        writer.append(key, keyValue.getValue());
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
