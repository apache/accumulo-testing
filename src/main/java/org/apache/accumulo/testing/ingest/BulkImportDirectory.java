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
package org.apache.accumulo.testing.ingest;

import java.io.IOException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.testing.cli.ClientOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

public class BulkImportDirectory {
  static class Opts extends ClientOpts {
    @Parameter(names = {"-t", "--table"}, required = true, description = "table to use")
    String tableName;
    @Parameter(names = {"-s", "--source"}, description = "directory to import from")
    String source = null;
    @Parameter(names = {"-f", "--failures"},
        description = "directory to copy failures into: will be deleted before the bulk import")
    String failures = null;
  }

  @SuppressWarnings("deprecation")
  public static void main(String[] args)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    final FileSystem fs = FileSystem.get(new Configuration());
    Opts opts = new Opts();
    System.err.println(
        "Deprecated syntax for BulkImportDirectory, please use the new style (see --help)");
    opts.parseArgs(BulkImportDirectory.class.getName(), args);
    fs.delete(new Path(opts.failures), true);
    fs.mkdirs(new Path(opts.failures));
    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      client.tableOperations().importDirectory(opts.tableName, opts.source, opts.failures, false);
    }
  }
}
