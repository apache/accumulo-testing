/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.testing.merkle.cli;

import java.io.FileNotFoundException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.testing.cli.ClientOpts;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * Accepts a set of tables, computes the hashes for each, and prints the top-level hash for each
 * table.
 * <p>
 * Will automatically create output tables for intermediate hashes instead of requiring their
 * existence. This will raise an exception when the table we want to use already exists.
 */
public class CompareTables {
  private static final Logger log = LoggerFactory.getLogger(CompareTables.class);

  public static class CompareTablesOpts extends ClientOpts {
    @Parameter(names = {"--tables"}, description = "Tables to compare", variableArity = true)
    public List<String> tables;

    @Parameter(names = {"-nt", "--numThreads"},
        description = "number of concurrent threads calculating digests")
    private int numThreads = 4;

    @Parameter(names = {"-hash", "--hash"}, required = true, description = "type of hash to use")
    private String hashName;

    @Parameter(names = {"-iter", "--iterator"}, description = "Should pushdown digest to iterators")
    private boolean iteratorPushdown = false;

    @Parameter(names = {"-s", "--splits"}, description = "File of splits to use for merkle tree")
    private String splitsFile = null;

    public List<String> getTables() {
      return this.tables;
    }

    public void setTables(List<String> tables) {
      this.tables = tables;
    }

    int getNumThreads() {
      return numThreads;
    }

    String getHashName() {
      return hashName;
    }

    boolean isIteratorPushdown() {
      return iteratorPushdown;
    }

    String getSplitsFile() {
      return splitsFile;
    }
  }

  private CompareTablesOpts opts;

  private CompareTables(CompareTablesOpts opts) {
    this.opts = opts;
  }

  private Map<String,String> computeAllHashes()
      throws AccumuloException, AccumuloSecurityException, TableExistsException,
      NoSuchAlgorithmException, TableNotFoundException, FileNotFoundException {
    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      final Map<String,String> hashesByTable = new HashMap<>();

      for (String table : opts.getTables()) {
        final String outputTableName = table + "_merkle";

        if (client.tableOperations().exists(outputTableName)) {
          throw new IllegalArgumentException(
              "Expected output table name to not yet exist: " + outputTableName);
        }

        client.tableOperations().create(outputTableName);

        GenerateHashes genHashes = new GenerateHashes();
        Collection<Range> ranges = genHashes.getRanges(client, table, opts.getSplitsFile());

        try {
          genHashes.run(client, table, table + "_merkle", opts.getHashName(), opts.getNumThreads(),
              opts.isIteratorPushdown(), ranges);
        } catch (Exception e) {
          log.error("Error generating hashes for {}", table, e);
          throw new RuntimeException(e);
        }

        ComputeRootHash computeRootHash = new ComputeRootHash();
        String hash = Hex
            .encodeHexString(computeRootHash.getHash(client, outputTableName, opts.getHashName()));

        hashesByTable.put(table, hash);
      }
      return hashesByTable;
    }
  }

  public static void main(String[] args) throws Exception {
    CompareTablesOpts opts = new CompareTablesOpts();
    opts.parseArgs("CompareTables", args);

    if (opts.isIteratorPushdown() && null != opts.getSplitsFile()) {
      throw new IllegalArgumentException(
          "Cannot use iterator pushdown with anything other than table split points");
    }

    CompareTables compareTables = new CompareTables(opts);
    Map<String,String> tableToHashes = compareTables.computeAllHashes();

    boolean hashesEqual = true;
    String previousHash = null;
    for (Entry<String,String> entry : tableToHashes.entrySet()) {
      // Set the previous hash if we dont' have one
      if (null == previousHash) {
        previousHash = entry.getValue();
      } else if (hashesEqual) {
        // If the hashes are still equal, check that the new hash is
        // also equal
        hashesEqual = previousHash.equals(entry.getValue());
      }

      System.out.println(entry.getKey() + " " + entry.getValue());
    }

    System.exit(hashesEqual ? 0 : 1);
  }
}
