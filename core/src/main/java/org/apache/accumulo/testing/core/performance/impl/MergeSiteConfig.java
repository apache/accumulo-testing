package org.apache.accumulo.testing.core.performance.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.hadoop.conf.Configuration;

public class MergeSiteConfig {
  public static void main(String[] args) throws Exception {
    String className = args[0];
    Path confFile = Paths.get(args[1], "accumulo-site.xml");

    PerformanceTest perfTest = Class.forName(className).asSubclass(PerformanceTest.class).newInstance();

    Configuration conf = new Configuration(false);
    byte[] newConf;


    try(BufferedInputStream in = new BufferedInputStream(Files.newInputStream(confFile))){
      conf.addResource(in);
      perfTest.getConfiguration().getAccumuloSite().forEach((k,v) -> conf.set(k, v));
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      conf.writeXml(baos);
      baos.close();
      newConf = baos.toByteArray();
    }


    Files.write(confFile, newConf);
  }
}
