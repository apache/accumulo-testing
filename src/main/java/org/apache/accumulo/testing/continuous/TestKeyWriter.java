package org.apache.accumulo.testing.continuous;

import java.io.IOException;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.hadoop.mapreduce.FileOutputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapreduce.FileOutputFormatBuilderImpl;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.ConfiguratorBase;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.FileOutputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestKeyWriter extends FileOutputFormat<TestKey,Value> {
  private static final Class<TestKeyWriter> CLASS = TestKeyWriter.class;

  public RecordWriter<TestKey,Value> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    final Configuration conf = context.getConfiguration();
    final AccumuloConfiguration acuConf = FileOutputConfigurator
        .getAccumuloConfiguration(AccumuloFileOutputFormat.class, context.getConfiguration());
    String extension = acuConf.get(Property.TABLE_FILE_TYPE);
    final Path file = this.getDefaultWorkFile(context, "." + extension);
    final int visCacheSize = ConfiguratorBase.getVisibilityCacheSize(conf);
    return new RecordWriter<TestKey,Value>() {
      RFileWriter out = null;

      public void close(TaskAttemptContext context) throws IOException {
        if (this.out != null) {
          this.out.close();
        }

      }

      public void write(TestKey key, Value value) throws IOException {
        if (this.out == null) {
          this.out = RFile.newWriter().to(file.toString()).withFileSystem(file.getFileSystem(conf))
              .withTableProperties(acuConf).withVisibilityCacheSize(visCacheSize).build();
          this.out.startDefaultLocalityGroup();
        }

        this.out.append(key.getKey(), value);
      }
    };
  }

  /**
   * Sets all the information required for this map reduce job.
   */
  public static FileOutputFormatBuilder.PathParams<JobConf> configure() {
    return new FileOutputFormatBuilderImpl<>(CLASS);
  }

}
