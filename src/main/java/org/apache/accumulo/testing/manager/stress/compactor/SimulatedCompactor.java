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
package org.apache.accumulo.testing.manager.stress.compactor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.random.RandomGenerator;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TNextCompactionJob;
import org.apache.accumulo.core.compaction.thrift.UnknownCompactionIdException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLock.LockLossReason;
import org.apache.accumulo.core.lock.ServiceLock.LockWatcher;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptor;
import org.apache.accumulo.core.lock.ServiceLockData.ServiceDescriptors;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.lock.ServiceLockSupport;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

/** A registered Compactor identity which polls the Manager without doing file compaction. */
final class SimulatedCompactor extends AbstractServer implements CompactorService.Iface {

  private static final Logger log = LoggerFactory.getLogger(SimulatedCompactor.class);

  static final class Counters {
    final LongAdder jobRequests = new LongAdder();
    final LongAdder idleResponses = new LongAdder();
    final LongAdder jobsReceived = new LongAdder();
    final LongAdder successes = new LongAdder();
    final LongAdder failures = new LongAdder();
    final LongAdder cancellations = new LongAdder();
    final LongAdder rpcFailures = new LongAdder();
    final LongAdder outcomeReportFailures = new LongAdder();
  }

  private final StressCompactorOptions options;
  private final ServerContext serverContext;
  private final int instanceNumber;
  private final UUID serverUuid = UUID.randomUUID();
  private final ResourceGroupId group;
  private final ClientContext clientContext;
  private final RandomGenerator random;
  private final AtomicBoolean stopping = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicBoolean cancelRequested = new AtomicBoolean();
  private final AtomicReference<String> currentId = new AtomicReference<>();
  private final AtomicReference<TExternalCompaction> currentCompaction = new AtomicReference<>();
  private final Semaphore wakeups = new Semaphore(0);
  private final Counters counters = new Counters();

  private ServerAddress serviceAddress;
  private ServiceLock serviceLock;
  private Thread worker;
  private volatile String advertisedAddress;

  SimulatedCompactor(StressCompactorOptions options, int instanceNumber) throws Exception {
    super(Type.COMPACTOR, new ServerOpts(), ServerContext::new, serverArgs(options));
    this.options = options;
    this.instanceNumber = instanceNumber;
    this.group = getResourceGroup();
    this.serverContext = getContext();
    ClientContext newClientContext;
    try {
      newClientContext = createClientContext(options.getClientProps());
    } catch (Exception e) {
      try {
        super.close();
      } catch (Exception cleanupError) {
        e.addSuppressed(cleanupError);
      }
      throw e;
    }
    this.clientContext = newClientContext;
    String configuredGroup =
        serverContext.getSiteConfiguration().get(Property.COMPACTOR_GROUP_NAME);
    if (!group.canonical().equals(configuredGroup)
        || !clientContext.getInstanceID().equals(serverContext.getInstanceID())) {
      try {
        clientContext.close();
      } finally {
        super.close();
      }
      throw new IllegalStateException("Simulated Compactor configuration mismatch: group="
          + configuredGroup + ", expectedGroup=" + group.canonical()
          + "; client and server contexts must also use the same Accumulo instance");
    }
    this.random = new java.util.Random(options.seed + instanceNumber * 0x9e3779b97f4a7c15L);
  }

  private static String[] serverArgs(StressCompactorOptions options) {
    String compactorGroup = ResourceGroupId.of(options.resourceGroup).canonical();
    return new String[] {"-o", Property.COMPACTOR_GROUP_NAME.getKey() + "=" + compactorGroup};
  }

  @Override
  protected String getResourceGroupPropertyValue(SiteConfiguration siteConfig) {
    return siteConfig.get(Property.COMPACTOR_GROUP_NAME);
  }

  @Override
  public ServiceLock getLock() {
    return serviceLock;
  }

  Counters getCounters() {
    return counters;
  }

  void start() throws Exception {
    var processor = ThriftProcessorTypes.getCompactorTProcessor(this,
        new ClientServiceHandler(serverContext), this, serverContext);
    serviceAddress = createThriftServer(processor);
    serviceAddress.startThriftServer("SimulatedCompactorService-" + instanceNumber);
    updateAdvertiseAddress(serviceAddress.address);
    advertisedAddress = serviceAddress.address.toString();

    ServiceLockPath lockPath =
        serverContext.getServerPaths().createCompactorPath(group, serviceAddress.address);
    ServiceLockSupport.createNonHaServiceLockPath(Type.COMPACTOR,
        serverContext.getZooSession().asReaderWriter(), lockPath);
    serviceLock = new ServiceLock(serverContext.getZooSession(), lockPath, serverUuid);
    ServiceDescriptors descriptors = new ServiceDescriptors();
    descriptors.addService(
        new ServiceDescriptor(serverUuid, ThriftService.CLIENT, advertisedAddress, group));
    descriptors.addService(
        new ServiceDescriptor(serverUuid, ThriftService.COMPACTOR, advertisedAddress, group));
    LockWatcher lockWatcher = new LockWatcher() {
      @Override
      public void lostLock(LockLossReason reason) {
        log.warn("Simulated Compactor {} lost its service registration: {}", instanceNumber,
            reason);
        stopping.set(true);
        wakeups.release();
      }

      @Override
      public void unableToMonitorLockNode(Exception e) {
        log.warn("Unable to monitor simulated Compactor {} service registration", instanceNumber,
            e);
        stopping.set(true);
        wakeups.release();
      }
    };
    if (!serviceLock.tryLock(lockWatcher, new ServiceLockData(descriptors))) {
      serviceAddress.server.stop();
      throw new IllegalStateException(
          "Could not register simulated Compactor at " + advertisedAddress);
    }

    log.info("Started simulated Compactor {} at {} in resource group {}", instanceNumber,
        advertisedAddress, group);
  }

  void startPolling() {
    worker = new Thread(this, "CompactorSimulator-" + instanceNumber);
    worker.setDaemon(false);
    worker.start();
  }

  @Override
  public void run() {
    runLoop();
  }

  private ServerAddress createThriftServer(org.apache.thrift.TProcessor processor) {
    var conf = serverContext.getConfiguration();
    var address = options.addresses();
    long threadTimeout =
        Math.max(10_000, conf.getTimeInMillis(Property.COMPACTOR_MINTHREADS_TIMEOUT));
    return TServerUtils.createThriftServer(conf, serverContext.getThriftServerType(), processor,
        serverContext.getInstanceID(), "SimulatedCompactor-" + instanceNumber, 1, threadTimeout,
        conf.getTimeInMillis(Property.COMPACTOR_THREADCHECK),
        conf.getAsBytes(Property.RPC_MAX_MESSAGE_SIZE), serverContext.getServerSslParams(),
        serverContext.getSaslParams(), serverContext.getClientTimeoutInMillis(),
        conf.getCount(Property.RPC_BACKLOG), serverContext.getMetricsInfo(),
        address.toArray(HostAndPort[]::new));
  }

  private static ClientContext createClientContext(java.util.Properties clientProperties) {
    AccumuloClient client = Accumulo.newClient().from(clientProperties).build();
    if (client instanceof ClientContext context) {
      return context;
    }
    try {
      client.close();
    } catch (Exception e) {
      log.warn("Could not close unsupported client implementation", e);
    }
    throw new IllegalStateException("Expected the Accumulo client builder to return ClientContext");
  }

  private void runLoop() {
    var metricsInfo = serverContext.getMetricsInfo();
    metricsInfo.addMetricsProducers(this);
    try {
      metricsInfo.init(MetricsInfo.serviceTags(serverContext.getInstanceName(),
          getApplicationName(), serviceAddress.address, group));
    } catch (IllegalStateException e) {
      // Catch this because it is thrown due to many Compactor instances in the same VM.
    }

    long minWait =
        serverContext.getConfiguration().getTimeInMillis(Property.COMPACTOR_MIN_JOB_WAIT_TIME);
    long maxWait =
        serverContext.getConfiguration().getTimeInMillis(Property.COMPACTOR_MAX_JOB_WAIT_TIME);
    while (!stopping.get()) {
      String externalId = ExternalCompactionId.generate(UUID.randomUUID()).canonical();
      boolean assignedJob = false;
      cancelRequested.set(false);
      currentId.set(externalId);
      counters.jobRequests.increment();
      try {
        TNextCompactionJob next = requestJob(externalId);
        TExternalCompactionJob job = next.getJob();
        if (!job.isSetExternalCompactionId()) {
          currentId.compareAndSet(externalId, null);
          counters.idleResponses.increment();
          waitForWork(calculateWait(next.getCompactorCount(), minWait, maxWait));
          continue;
        }

        counters.jobsReceived.increment();
        assignedJob = true;
        if (!externalId.equals(job.getExternalCompactionId())) {
          throw new IllegalStateException("Manager returned compaction ID "
              + job.getExternalCompactionId() + " for request " + externalId);
        }
        TExternalCompaction running = new TExternalCompaction();
        running.setCompactor(advertisedAddress);
        running.setGroupName(group.canonical());
        long startTime = System.currentTimeMillis();
        running.setStartTime(startTime);
        running.putToUpdates(startTime, new TCompactionStatusUpdate(TCompactionState.STARTED,
            "Simulated compaction started", -1, -1, -1, 0));
        running.setJob(job);
        currentCompaction.set(running);

        CompactionOutcome outcome = CompactionOutcome.choose(options, random);
        if (cancelRequested.get()) {
          outcome = CompactionOutcome.CANCELLATION;
        }
        reportOutcome(job, outcome);
      } catch (Exception e) {
        counters.rpcFailures.increment();
        if (assignedJob) {
          counters.outcomeReportFailures.increment();
        }
        log.warn("Simulated Compactor {} failed a Manager RPC", instanceNumber, e);
        waitForWork(minWait);
      } finally {
        currentCompaction.set(null);
        currentId.set(null);
        cancelRequested.set(false);
      }
    }
  }

  private TNextCompactionJob requestJob(String externalId) throws Exception {
    var coordinator = ExternalCompactionUtil.getCoordinatorClient(clientContext);
    try {
      return coordinator.getCompactionJob(TraceUtil.traceInfo(), clientContext.rpcCreds(),
          group.canonical(), advertisedAddress, externalId);
    } finally {
      ThriftUtil.returnClient(coordinator, clientContext);
    }
  }

  private void reportOutcome(TExternalCompactionJob job, CompactionOutcome outcome)
      throws Exception {
    var coordinator = ExternalCompactionUtil.getCoordinatorClient(clientContext);
    try {
      switch (outcome) {
        case SUCCESS -> {
          TCompactionStats stats = new TCompactionStats();
          stats.setEntriesRead(0);
          stats.setEntriesWritten(0);
          stats.setFileSize(0);
          coordinator.compactionCompleted(TraceUtil.traceInfo(), clientContext.rpcCreds(),
              job.getExternalCompactionId(), job.getExtent(), stats, group.canonical(),
              advertisedAddress);
          counters.successes.increment();
        }
        case FAILURE -> {
          coordinator.compactionFailed(TraceUtil.traceInfo(), clientContext.rpcCreds(),
              job.getExternalCompactionId(), job.getExtent(), "simulated compaction failure",
              TCompactionState.FAILED, group.canonical(), advertisedAddress);
          counters.failures.increment();
        }
        case CANCELLATION -> {
          coordinator.compactionFailed(TraceUtil.traceInfo(), clientContext.rpcCreds(),
              job.getExternalCompactionId(), job.getExtent(), "simulated compaction cancellation",
              TCompactionState.CANCELLED, group.canonical(), advertisedAddress);
          counters.cancellations.increment();
        }
      }
      TCompactionState state = switch (outcome) {
        case SUCCESS -> TCompactionState.SUCCEEDED;
        case FAILURE -> TCompactionState.FAILED;
        case CANCELLATION -> TCompactionState.CANCELLED;
      };
      log.info("Simulated compaction {} finished with state {}", job.getExternalCompactionId(),
          state);
    } finally {
      ThriftUtil.returnClient(coordinator, clientContext);
    }
  }

  private static long calculateWait(int compactorCount, long minWait, long maxWait) {
    long wait = Math.max(minWait, (long) compactorCount * minWait / 3);
    wait = Math.min(maxWait, wait);
    return (long) (wait * (0.9 + 0.2 * ThreadLocalRandom.current().nextDouble()));
  }

  private void waitForWork(long millis) {
    if (stopping.get()) {
      return;
    }
    try {
      wakeups.tryAcquire(Math.max(1, millis), MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      stopping.set(true);
    }
  }

  private void requireSystemAction(TCredentials credentials) throws ThriftSecurityException {
    if (!serverContext.getSecurityOperation().canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
  }

  @Override
  public void gracefulShutdown(TCredentials credentials) {
    super.gracefulShutdown(credentials);
    if (isShutdownRequested()) {
      stopping.set(true);
      wakeups.release();
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    stopping.set(true);
    wakeups.release();
    if (worker != null) {
      worker.interrupt();
      try {
        worker.join(10_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (serviceLock != null) {
      try {
        serviceLock.unlock();
      } catch (KeeperException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        log.warn("Could not unregister simulated Compactor {}", advertisedAddress, e);
      }
    }
    if (serviceAddress != null) {
      serviceAddress.server.stop();
    }
    try {
      clientContext.close();
    } catch (Exception e) {
      log.warn("Could not close client context for simulated Compactor {}", instanceNumber, e);
    }
    super.close();
    log.info(
        "Compactor {} totals: polls={}, idle={}, jobs={}, success={}, failure={}, cancelled={}, "
            + "rpcFailures={}, outcomeReportFailures={}",
        instanceNumber, counters.jobRequests.sum(), counters.idleResponses.sum(),
        counters.jobsReceived.sum(), counters.successes.sum(), counters.failures.sum(),
        counters.cancellations.sum(), counters.rpcFailures.sum(),
        counters.outcomeReportFailures.sum());
  }

  @Override
  public TExternalCompaction getRunningCompaction(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    requireSystemAction(credentials);
    TExternalCompaction current = currentCompaction.get();
    return current == null ? new TExternalCompaction() : new TExternalCompaction(current);
  }

  @Override
  public String getRunningCompactionId(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    requireSystemAction(credentials);
    String id = currentId.get();
    return id == null ? "" : id;
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    requireSystemAction(credentials);
    return List.of();
  }

  public void wake(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException {
    requireSystemAction(credentials);
    wakeups.release();
  }

  @Override
  public void cancel(TInfo tinfo, TCredentials credentials, String externalCompactionId)
      throws TException {
    requireSystemAction(credentials);
    if (externalCompactionId.equals(currentId.get())) {
      cancelRequested.set(true);
    } else {
      throw new UnknownCompactionIdException();
    }
  }

}
