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
package org.apache.accumulo.testing.ingest;

import java.io.IOException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.testing.cli.ClientOpts;

import com.beust.jcommander.Parameter;

public class BulkImportDirectory {
  static class Opts extends ClientOpts {
    @Parameter(names = {"-t", "--table"}, required = true, description = "table to use")
    String tableName;
    @Parameter(names = {"-s", "--source"}, description = "directory to import from")
    String source = null;
  }

  public static void main(String[] args)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Opts opts = new Opts();
    opts.parseArgs(BulkImportDirectory.class.getName(), args);
    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      client.tableOperations().importDirectory(opts.source).to(opts.tableName).tableTime(false)
          .load();
    }
  }
}
