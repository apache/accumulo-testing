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

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.cli.ClientOpts;
import org.apache.accumulo.testing.merkle.MerkleTree;
import org.apache.accumulo.testing.merkle.MerkleTreeNode;
import org.apache.accumulo.testing.merkle.RangeSerialization;
import org.apache.commons.codec.binary.Hex;

import com.beust.jcommander.Parameter;

/**
 * Given a table created by {@link GenerateHashes} which contains the leaves of a Merkle tree,
 * compute the root node of the Merkle tree which can be quickly compared to the root node of
 * another Merkle tree to ascertain equality.
 */
public class ComputeRootHash {

  public static class ComputeRootHashOpts extends ClientOpts {
    @Parameter(names = {"-t", "--table"}, required = true, description = "table to use")
    String tableName;
    @Parameter(names = {"-hash", "--hash"}, required = true, description = "type of hash to use")
    String hashName;
  }

  private byte[] getHash(ComputeRootHashOpts opts)
      throws TableNotFoundException, NoSuchAlgorithmException {
    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      return getHash(client, opts.tableName, opts.hashName);
    }
  }

  byte[] getHash(AccumuloClient client, String table, String hashName)
      throws TableNotFoundException, NoSuchAlgorithmException {
    List<MerkleTreeNode> leaves = getLeaves(client, table);

    MerkleTree tree = new MerkleTree(leaves, hashName);

    return tree.getRootNode().getHash();
  }

  private ArrayList<MerkleTreeNode> getLeaves(AccumuloClient client, String tableName)
      throws TableNotFoundException {
    // TODO make this a bit more resilient to very large merkle trees by
    // lazily reading more data from the table when necessary
    final Scanner s = client.createScanner(tableName, Authorizations.EMPTY);
    final ArrayList<MerkleTreeNode> leaves = new ArrayList<>();

    for (Entry<Key,Value> entry : s) {
      Range range = RangeSerialization.toRange(entry.getKey());
      byte[] hash = entry.getValue().get();

      leaves.add(new MerkleTreeNode(range, 0, Collections.emptyList(), hash));
    }

    return leaves;
  }

  public static void main(String[] args) throws Exception {
    ComputeRootHashOpts opts = new ComputeRootHashOpts();
    opts.parseArgs("ComputeRootHash", args);

    ComputeRootHash computeRootHash = new ComputeRootHash();
    byte[] rootHash = computeRootHash.getHash(opts);

    System.out.println(Hex.encodeHexString(rootHash));
  }
}
