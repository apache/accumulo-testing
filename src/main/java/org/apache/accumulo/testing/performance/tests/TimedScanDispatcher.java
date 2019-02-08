package org.apache.accumulo.testing.performance.tests;

import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.core.spi.scan.ScanInfo;

public class TimedScanDispatcher implements ScanDispatcher {

  String quickExecutor;
  long quickTime;

  String longExectuor;

  public void init(InitParameters params) {
    quickExecutor = params.getOptions().get("quick.executor");
    quickTime = Long.parseLong(params.getOptions().get("quick.time.ms"));

    longExectuor = params.getOptions().get("long.executor");
  }

  @Override
  public String dispatch(DispatchParmaters params) {
    ScanInfo scanInfo = params.getScanInfo();

    if (scanInfo.getRunTimeStats().sum() < quickTime)
      return quickExecutor;

    return longExectuor;
  }
}
