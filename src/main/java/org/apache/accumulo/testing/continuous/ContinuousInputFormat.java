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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.access.AccessExpression.unquote;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AND;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AUTHORIZATION;
import static org.apache.accumulo.testing.continuous.ContinuousIngest.genCol;
import static org.apache.accumulo.testing.continuous.ContinuousIngest.genLong;
import static org.apache.accumulo.testing.continuous.ContinuousIngest.genRow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.ParsedAccessExpression;
import org.apache.accumulo.access.ParsedAccessExpression.ExpressionType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Generates a continuous ingest linked list per map reduce split. Each linked list is of
 * configurable length.
 */
public class ContinuousInputFormat extends InputFormat<Key,Value> {

  /**
   * As part of normalizing access expression this class is used to sort and dedupe sub-expressions
   * in a tree set.
   */
  private static class NormalizedExpression implements Comparable<NormalizedExpression> {
    public final String expression;
    public final ExpressionType type;

    NormalizedExpression(String expression, ExpressionType type) {
      this.expression = expression;
      this.type = type;
    }

    // determines the sort order of different kinds of subexpressions.
    private static int typeOrder(ExpressionType type) {
      switch (type) {
        case AUTHORIZATION:
          return 1;
        case OR:
          return 2;
        case AND:
          return 3;
        default:
          throw new IllegalArgumentException("Unexpected type " + type);
      }
    }

    @Override
    public int compareTo(NormalizedExpression o) {
      // Changing this comparator would significantly change how expressions are normalized.
      int cmp = typeOrder(type) - typeOrder(o.type);
      if (cmp == 0) {
        if (type == AUTHORIZATION) {
          // sort based on the unquoted and unescaped form of the authorization
          cmp = unquote(expression).compareTo(unquote(o.expression));
        } else {
          cmp = expression.compareTo(o.expression);
        }

      }
      return cmp;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof NormalizedExpression) {
        return compareTo((NormalizedExpression) o) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return expression.hashCode();
    }
  }

  /**
   * This method helps with the flattening aspect of normalization by recursing down as far as
   * possible the parse tree in the case when the expression type is the same. As long as the type
   * is the same in the sub expression, keep using the same tree set.
   */
  private static void flatten(ExpressionType parentType, ParsedAccessExpression parsed,
      TreeSet<NormalizedExpression> normalizedExpressions) {
    if (parsed.getType() == parentType) {
      for (var child : parsed.getChildren()) {
        flatten(parentType, child, normalizedExpressions);
      }
    } else {
      // The type changed, so start again on the subexpression.
      normalizedExpressions.add(normalize(parsed));
    }
  }

  /**
   * <p>
   * For a given access expression this example will deduplicate, sort, flatten, and remove unneeded
   * parentheses or quotes in the expressions. The following list gives examples of what each
   * normalization step does.
   *
   * <ul>
   * <li>As an example of flattening, the expression {@code A&(B&C)} flattens to {@code
   * A&B&C}.</li>
   * <li>As an example of sorting, the expression {@code (Z&Y)|(C&B)} sorts to {@code
   * (B&C)|(Y&Z)}</li>
   * <li>As an example of deduplication, the expression {@code X&Y&X} normalizes to {@code X&Y}</li>
   * <li>As an example of unneeded quotes, the expression {@code "ABC"&"XYZ"} normalizes to
   * {@code ABC&XYZ}</li>
   * <li>As an example of unneeded parentheses, the expression {@code (((ABC)|(XYZ)))} normalizes to
   * {@code ABC|XYZ}</li>
   * </ul>
   *
   * <p>
   * This algorithm attempts to have the same behavior as the one in the Accumulo 2.1
   * ColumnVisibility class. However the implementation is very different.
   * </p>
   */
  private static NormalizedExpression normalize(ParsedAccessExpression parsed) {
    if (parsed.getType() == AUTHORIZATION) {
      // If the authorization is quoted and it does not need to be quoted then the following two
      // lines will remove the unnecessary quoting.
      String unquoted = AccessExpression.unquote(parsed.getExpression());
      String quoted = AccessExpression.quote(unquoted);
      return new NormalizedExpression(quoted, parsed.getType());
    } else {
      // The tree set does the work of sorting and deduplicating sub expressions.
      TreeSet<NormalizedExpression> normalizedChildren = new TreeSet<>();
      for (var child : parsed.getChildren()) {
        flatten(parsed.getType(), child, normalizedChildren);
      }

      if (normalizedChildren.size() == 1) {
        return normalizedChildren.first();
      } else {
        String operator = parsed.getType() == AND ? "&" : "|";
        String sep = "";

        StringBuilder builder = new StringBuilder();

        for (var child : normalizedChildren) {
          builder.append(sep);
          if (child.type == AUTHORIZATION) {
            builder.append(child.expression);
          } else {
            builder.append("(");
            builder.append(child.expression);
            builder.append(")");
          }
          sep = operator;
        }

        return new NormalizedExpression(builder.toString(), parsed.getType());
      }
    }
  }

  private static final String PROP_UUID = "mrbulk.uuid";
  private static final String PROP_MAP_TASK = "mrbulk.map.task";
  private static final String PROP_MAP_NODES = "mrbulk.map.nodes";
  private static final String PROP_ROW_MIN = "mrbulk.row.min";
  private static final String PROP_ROW_MAX = "mrbulk.row.max";
  private static final String PROP_FAM_MAX = "mrbulk.fam.max";
  private static final String PROP_QUAL_MAX = "mrbulk.qual.max";
  private static final String PROP_CHECKSUM = "mrbulk.checksum";
  private static final String PROP_VIS = "mrbulk.vis";

  private static class RandomSplit extends InputSplit implements Writable {
    @Override
    public void write(DataOutput dataOutput) {}

    @Override
    public void readFields(DataInput dataInput) {}

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return new String[0];
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    int numTask = jobContext.getConfiguration().getInt(PROP_MAP_TASK, 1);
    return Stream.generate(RandomSplit::new).limit(numTask).collect(Collectors.toList());
  }

  public static void configure(Configuration conf, String uuid, ContinuousEnv env) {
    conf.set(PROP_UUID, uuid);
    conf.setInt(PROP_MAP_TASK, env.getBulkMapTask());
    conf.setLong(PROP_MAP_NODES, env.getBulkMapNodes());
    conf.setLong(PROP_ROW_MIN, env.getRowMin());
    conf.setLong(PROP_ROW_MAX, env.getRowMax());
    conf.setInt(PROP_FAM_MAX, env.getMaxColF());
    conf.setInt(PROP_QUAL_MAX, env.getMaxColQ());
    conf.setBoolean(PROP_CHECKSUM,
        Boolean.parseBoolean(env.getTestProperty(TestProps.CI_INGEST_CHECKSUM)));
    conf.set(PROP_VIS, env.getTestProperty(TestProps.CI_INGEST_VISIBILITIES));
  }

  @Override
  public RecordReader<Key,Value> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) {
    return new RecordReader<Key,Value>() {
      long numNodes;
      long nodeCount;
      private Random random;

      private byte[] uuid;

      long minRow;
      long maxRow;
      int maxFam;
      int maxQual;
      List<ColumnVisibility> visibilities;
      boolean checksum;

      Key prevKey;
      Key currKey;
      Value currValue;

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext job) {
        numNodes = job.getConfiguration().getLong(PROP_MAP_NODES, 1000000);
        uuid = job.getConfiguration().get(PROP_UUID).getBytes(UTF_8);

        minRow = job.getConfiguration().getLong(PROP_ROW_MIN, 0);
        maxRow = job.getConfiguration().getLong(PROP_ROW_MAX, Long.MAX_VALUE);
        maxFam = job.getConfiguration().getInt(PROP_FAM_MAX, Short.MAX_VALUE);
        maxQual = job.getConfiguration().getInt(PROP_QUAL_MAX, Short.MAX_VALUE);
        checksum = job.getConfiguration().getBoolean(PROP_CHECKSUM, false);
        visibilities = ContinuousIngest.parseVisibilities(job.getConfiguration().get(PROP_VIS));

        random = new Random(new SecureRandom().nextLong());

        nodeCount = 0;
      }

      private Key genKey(CRC32 cksum) {

        byte[] row = genRow(genLong(minRow, maxRow, random));

        byte[] fam = genCol(random.nextInt(maxFam));
        byte[] qual = genCol(random.nextInt(maxQual));
        byte[] cv = visibilities.get(random.nextInt(visibilities.size())).getExpression();
        cv = normalize(AccessExpression.parse(cv)).expression.getBytes(UTF_8);

        if (cksum != null) {
          cksum.update(row);
          cksum.update(fam);
          cksum.update(qual);
          cksum.update(cv);
        }

        return new Key(row, fam, qual, cv);
      }

      private byte[] createValue(byte[] ingestInstanceId, byte[] prevRow, Checksum cksum) {
        return ContinuousIngest.createValue(ingestInstanceId, nodeCount, prevRow, cksum);
      }

      @Override
      public boolean nextKeyValue() {

        if (nodeCount < numNodes) {
          CRC32 cksum = checksum ? new CRC32() : null;
          prevKey = currKey;
          byte[] prevRow = prevKey != null ? prevKey.getRowData().toArray() : null;
          currKey = genKey(cksum);
          currValue = new Value(createValue(uuid, prevRow, cksum));

          nodeCount++;
          return true;
        } else {
          return false;
        }
      }

      @Override
      public Key getCurrentKey() {
        return currKey;
      }

      @Override
      public Value getCurrentValue() {
        return currValue;
      }

      @Override
      public float getProgress() {
        return nodeCount * 1.0f / numNodes;
      }

      @Override
      public void close() throws IOException {

      }
    };
  }
}
