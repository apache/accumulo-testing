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
package org.apache.accumulo.testing.continuous;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.cli.ClientOpts;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * BUGS This code does not handle the fact that these files could include log events from previous
 * months. It therefore it assumes all dates are in the current month. One solution might be to skip
 * log files that haven't been touched in the last month, but that doesn't prevent newer files that
 * have old dates in them.
 */
public class UndefinedAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(UndefinedAnalyzer.class);

  static class UndefinedNode {

    UndefinedNode(String undef2, String ref2) {
      this.undef = undef2;
      this.ref = ref2;
    }

    String undef;
    String ref;
  }

  static class IngestInfo {

    Map<String,TreeMap<Long,Long>> flushes = new HashMap<>();

    IngestInfo(String logDir) throws Exception {
      File dir = new File(logDir);
      File[] ingestLogs = dir.listFiles((dir1, name) -> name.endsWith("ingest.out"));

      if (ingestLogs != null) {
        for (File log : ingestLogs) {
          parseLog(log);
        }
      }
    }

    private void parseLog(File log) throws Exception {
      String line;
      TreeMap<Long,Long> tm = null;
      try (BufferedReader reader = Files.newBufferedReader(log.toPath())) {
        while ((line = reader.readLine()) != null) {
          if (!line.startsWith("UUID"))
            continue;
          String[] tokens = line.split("\\s");
          String time = tokens[1];
          String uuid = tokens[2];

          if (flushes.containsKey(uuid)) {
            logger.error("WARN Duplicate uuid " + log);
            return;
          }

          tm = new TreeMap<>(Collections.reverseOrder());
          tm.put(0L, Long.parseLong(time));
          flushes.put(uuid, tm);
          break;

        }
        if (tm == null) {
          logger.error("WARN Bad ingest log " + log);
          return;
        }

        while ((line = reader.readLine()) != null) {
          String[] tokens = line.split("\\s");

          if (!tokens[0].equals("FLUSH"))
            continue;

          String time = tokens[1];
          String count = tokens[4];

          tm.put(Long.parseLong(count), Long.parseLong(time));
        }
      }
    }

    Iterator<Long> getTimes(String uuid, long count) {
      TreeMap<Long,Long> tm = flushes.get(uuid);

      if (tm == null)
        return null;

      return tm.tailMap(count).values().iterator();
    }
  }

  static class TabletAssignment {
    String tablet;
    String endRow;
    String prevEndRow;
    String server;
    long time;

    TabletAssignment(String tablet, String er, String per, String server, long time) {
      this.tablet = tablet;
      this.endRow = er;
      this.prevEndRow = per;
      this.server = server;
      this.time = time;
    }

    public boolean contains(String row) {
      return prevEndRow.compareTo(row) < 0 && endRow.compareTo(row) >= 0;
    }
  }

  static class TabletHistory {

    List<TabletAssignment> assignments = new ArrayList<>();

    TabletHistory(String tableId, String acuLogDir) throws Exception {
      // looks for a file called load_events.log... the expected format of lines in this file is
      // <date> <time> <tablet> <tserver>
      //
      // These lines represent when tablets were loaded, this file may be produced from master logs
      // with following command :
      //
      // grep -h "was loaded on" *master*debug* | cut -d ' ' -f 1,2,7,11 | sort > load_events.log
      //
      // The command may need to be adjusted if formatting changes.

      File dir = new File(acuLogDir);
      File[] masterLogs = dir.listFiles((dir1, name) -> name.matches("load_events.log"));

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

      if (masterLogs != null) {
        for (File masterLog : masterLogs) {

          String line;
          try (BufferedReader reader = Files.newBufferedReader(masterLog.toPath())) {
            while ((line = reader.readLine()) != null) {
              String[] tokens = line.split("\\s+");
              String day = tokens[0];
              String time = tokens[1];
              String tablet = tokens[2];
              String server = tokens[3];

              int pos1 = -1;
              int pos2 = -1;
              int pos3 = -1;

              for (int i = 0; i < tablet.length(); i++) {
                if (tablet.charAt(i) == '<' || tablet.charAt(i) == ';') {
                  if (pos1 == -1) {
                    pos1 = i;
                  } else if (pos2 == -1) {
                    pos2 = i;
                  } else {
                    pos3 = i;
                  }
                }
              }

              if (pos1 > 0 && pos2 > 0 && pos3 == -1) {
                String tid = tablet.substring(0, pos1);
                String endRow = tablet.charAt(pos1) == '<' ? "8000000000000000"
                    : tablet.substring(pos1 + 1, pos2);
                String prevEndRow = tablet.charAt(pos2) == '<' ? "" : tablet.substring(pos2 + 1);
                if (tid.equals(tableId)) {
                  Date date = sdf.parse(day + " " + time);
                  assignments.add(
                      new TabletAssignment(tablet, endRow, prevEndRow, server, date.getTime()));

                }
              } else if (!tablet.startsWith("!0")) {
                logger.error("Cannot parse tablet {}", tablet);
              }
            }
          }
        }
      }
    }

    TabletAssignment findMostRecentAssignment(String row, long time1, long time2) {

      long latest = Long.MIN_VALUE;
      TabletAssignment ret = null;

      for (TabletAssignment assignment : assignments) {
        if (assignment.contains(row) && assignment.time <= time2 && assignment.time > latest) {
          latest = assignment.time;
          ret = assignment;
        }
      }

      return ret;
    }
  }

  static class Opts extends ClientOpts {
    @Parameter(names = "--logdir", description = "directory containing the log files",
        required = true)
    String logDir;
    @Parameter(names = {"-t", "--table"}, description = "table to use")
    String tableName = "ci";
  }

  /**
   * Class to analyze undefined references and accumulo logs to isolate the time/tablet where data
   * was lost.
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(UndefinedAnalyzer.class.getName(), args);

    List<UndefinedNode> undefs = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split("\\s");
        String undef = tokens[0];
        String ref = tokens[1];

        undefs.add(new UndefinedNode(undef, ref));
      }
    }

    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build();
        BatchScanner bscanner = client.createBatchScanner(opts.tableName, opts.auths)) {
      List<Range> refs = new ArrayList<>();

      for (UndefinedNode undefinedNode : undefs)
        refs.add(new Range(new Text(undefinedNode.ref)));

      bscanner.setRanges(refs);

      HashMap<String,List<String>> refInfo = new HashMap<>();

      for (Entry<Key,Value> entry : bscanner) {
        String ref = entry.getKey().getRow().toString();
        List<String> vals = refInfo.computeIfAbsent(ref, k -> new ArrayList<>());
        vals.add(entry.getValue().toString());
      }

      IngestInfo ingestInfo = new IngestInfo(opts.logDir);
      String tableId = client.tableOperations().tableIdMap().get(opts.tableName);
      TabletHistory tabletHistory = new TabletHistory(tableId, opts.logDir);

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

      for (UndefinedNode undefinedNode : undefs) {

        List<String> refVals = refInfo.get(undefinedNode.ref);
        if (refVals != null) {
          for (String refVal : refVals) {
            TabletAssignment ta = null;

            String[] tokens = refVal.split(":");

            String uuid = tokens[0];
            String count = tokens[1];

            String t1 = "";
            String t2 = "";

            Iterator<Long> times = ingestInfo.getTimes(uuid, Long.parseLong(count, 16));
            if (times != null) {
              if (times.hasNext()) {
                long time2 = times.next();
                t2 = sdf.format(new Date(time2));
                if (times.hasNext()) {
                  long time1 = times.next();
                  t1 = sdf.format(new Date(time1));
                  ta = tabletHistory.findMostRecentAssignment(undefinedNode.undef, time1, time2);
                }
              }
            }

            if (ta == null)
              logger.debug("{} {} {} {} {}", undefinedNode.undef, undefinedNode.ref, uuid, t1, t2);
            else
              logger.debug("{} {} {} {} {}", undefinedNode.undef, undefinedNode.ref, ta.tablet,
                  ta.server, uuid, t1, t2);

          }
        } else {
          logger.debug("{} {}", undefinedNode.undef, undefinedNode.ref);
        }
      }
    }
  }
}
