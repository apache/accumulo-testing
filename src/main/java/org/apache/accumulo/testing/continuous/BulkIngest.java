package org.apache.accumulo.testing.continuous;

import static org.apache.accumulo.testing.TestProps.CI_BULK_UUID;
import static org.apache.accumulo.testing.continuous.ContinuousIngest.genCol;
import static org.apache.accumulo.testing.continuous.ContinuousIngest.genLong;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.hadoop.mapreduce.partition.KeyRangePartitioner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk import a million random key value pairs. Same format as ContinuousIngest and can be
 * verified by running ContinuousVerify.
 */
public class BulkIngest extends Configured implements Tool {
  public static final int NUM_KEYS = 1_000_000;
  public static final String BULK_CI_DIR = "ci-bulk";

  public static final Logger log = LoggerFactory.getLogger(BulkIngest.class);

  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJarByClass(BulkIngest.class);
    // very important to prevent guava conflicts
    job.getConfiguration().set("mapreduce.job.classloader", "true");
    FileSystem fs = FileSystem.get(job.getConfiguration());

    final String JOB_DIR = BULK_CI_DIR + "/" + getCurrentJobNumber(fs);
    final String RFILE_DIR = JOB_DIR + "/rfiles";
    log.info("Creating new job at {}", JOB_DIR);

    String ingestInstanceId = UUID.randomUUID().toString();
    job.getConfiguration().set(CI_BULK_UUID, ingestInstanceId);

    log.info(String.format("UUID %d %s", System.currentTimeMillis(), ingestInstanceId));

    Path outputDir = new Path(RFILE_DIR);

    job.setInputFormatClass(RandomInputFormat.class);

    // map the generated random longs to key values
    job.setMapperClass(RandomMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(Value.class);

    // output RFiles for the import
    job.setOutputFormatClass(AccumuloFileOutputFormat.class);
    AccumuloFileOutputFormat.configure().outputPath(outputDir).store(job);

    try (ContinuousEnv env = new ContinuousEnv(args)) {
      String tableName = env.getAccumuloTableName();

      // create splits file for KeyRangePartitioner
      String splitsFile = JOB_DIR + "/splits.txt";
      try (AccumuloClient client = env.getAccumuloClient()) {

        // make sure splits file is closed before continuing
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(
            splitsFile))))) {
          Collection<Text> splits = client.tableOperations().listSplits(tableName, 100);
          for (Text split : splits) {
            out.println(Base64.getEncoder().encodeToString(split.copyBytes()));
          }
          job.setNumReduceTasks(splits.size() + 1);
        }

        job.setPartitionerClass(KeyRangePartitioner.class);
        KeyRangePartitioner.setSplitFile(job, splitsFile);

        job.waitForCompletion(true);
        boolean success = job.isSuccessful();

        // bulk import completed files
        if (success) {
          log.info("Sort and create job successful. Bulk importing {} to {}", RFILE_DIR, tableName);
          client.tableOperations().importDirectory(RFILE_DIR).to(tableName).load();
        } else {
          log.error("Job failed, not calling bulk import");
        }
        return success ? 0 : 1;
      }
    }
  }

  private int getCurrentJobNumber(FileSystem fs) throws Exception {
    Path jobPath = new Path(BULK_CI_DIR);
    FileStatus jobDir = fs.getFileStatus(jobPath);
    if (jobDir.isDirectory()) {
      FileStatus[] jobs = fs.listStatus(jobPath);
      return jobs.length;
    } else {
      log.info("{} directory doesn't exist yet, first job running will create it.", BULK_CI_DIR);
      return 0;
    }
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new BulkIngest(), args);
    System.exit(ret);
  }

  /**
   * Mapper that takes the longs from RandomInputFormat and output Key Value pairs
   */
  public static class RandomMapper extends Mapper<LongWritable,LongWritable,Key,Value> {

    private String uuid;
    private Text currentRow;
    private Text currentValue;
    private Text emptyCfCq;

    @Override
    protected void setup(Context context) {
      uuid = context.getConfiguration().get(CI_BULK_UUID);
      currentRow = new Text();
      currentValue = new Text();
      emptyCfCq = new Text(genCol(0));
    }

    @Override
    protected void map(LongWritable key, LongWritable value, Context context) throws IOException,
        InterruptedException {
      currentRow.set(ContinuousIngest.genRow(key.get()));

      // hack since we can't pass null - don't set first val (prevRow), we want it to be null
      long longVal = value.get();
      if (longVal != 1L) {
        currentValue.set(ContinuousIngest.genRow(longVal));
      }

      Key outputKey = new Key(currentRow, emptyCfCq, emptyCfCq);
      Value outputValue = ContinuousIngest.createValue(uuid.getBytes(), 0,
          currentValue.copyBytes(), null);

      context.write(outputKey, outputValue);
    }
  }

  /**
   * Generates a million LongWritable keys.  The LongWritable value points to the previous key.
   * The first key value pair has a value of 1L.  This is translated to null in RandomMapper
   */
  public static class RandomInputFormat extends InputFormat {

    public static class RandomSplit extends InputSplit implements Writable {
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
      List<InputSplit> splits = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        splits.add(new RandomSplit());
      }
      return splits;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit,
        TaskAttemptContext taskAttemptContext) {
      return new RecordReader() {
        int number;
        int currentNumber;
        LongWritable currentKey;
        LongWritable prevRow;
        private Random random;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext job) {
          // number = Integer.parseInt(job.getConfiguration().get(NUM_KEYS));
          number = NUM_KEYS;
          currentKey = new LongWritable(1);
          prevRow = new LongWritable(1);
          random = new Random();
          currentNumber = 0;
        }

        @Override
        public boolean nextKeyValue() {
          if (currentNumber < number) {
            prevRow.set(currentKey.get());
            currentKey.set(genLong(0, Long.MAX_VALUE, random));
            currentNumber++;
            return true;
          } else {
            return false;
          }
        }

        @Override
        public LongWritable getCurrentKey() {
          return currentKey;
        }

        @Override
        public LongWritable getCurrentValue() {
          return prevRow;
        }

        @Override
        public float getProgress() {
          return currentNumber * 1.0f / number;
        }

        @Override
        public void close() throws IOException {

        }
      };
    }
  }
}
