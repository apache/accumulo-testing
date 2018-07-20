package org.apache.accumulo.testing.core.performance.impl;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.testing.core.performance.Environment;
import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.accumulo.testing.core.performance.Report;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PerfTestRunner {
  public static void main(String[] args) throws Exception {
    String clientProps = args[0];
    String className = args[1];
    String accumuloVersion = args[2];
    String outputDir = args[3];

    PerformanceTest perfTest = Class.forName(className).asSubclass(PerformanceTest.class).newInstance();

    Connector conn = Connector.builder().usingProperties(clientProps).build();

    Instant start = Instant.now();

    Report result = perfTest.runTest(new Environment() {
      @Override
      public Connector getConnector() {
        return conn;
      }
    });

    Instant stop = Instant.now();

    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    ContextualReport report = new ContextualReport(className, accumuloVersion, start, stop, result);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    String time = Instant.now().atZone(ZoneId.systemDefault()).format(formatter);
    Path outputFile = Paths.get(outputDir, perfTest.getClass().getSimpleName() + "_" + time + ".json");

    try (Writer writer = Files.newBufferedWriter(outputFile)) {
      gson.toJson(report, writer);
    }
  }
}
