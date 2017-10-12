/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.thrift;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.Api_Constants;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.ExplicitReconciliationSettings;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.PulseJobUpdateResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.StartJobUpdateResult;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.reconciliation.TaskReconciler;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.JobUpdateController.AuditData;
import org.apache.aurora.scheduler.updater.JobUpdateController.JobUpdatingException;
import org.apache.aurora.scheduler.updater.UpdateInProgressException;
import org.apache.aurora.scheduler.updater.UpdateStateException;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.gen.Resource.withDiskMb;
import static org.apache.aurora.gen.Resource.withNumCpus;
import static org.apache.aurora.gen.Resource.withRamMb;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.JOB_UPDATING_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import static org.apache.aurora.scheduler.thrift.Fixtures.CRON_JOB;
import static org.apache.aurora.scheduler.thrift.Fixtures.ENOUGH_QUOTA;
import static org.apache.aurora.scheduler.thrift.Fixtures.IDENTITY;
import static org.apache.aurora.scheduler.thrift.Fixtures.INSTANCE_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.INVALID_TASK_CONFIG;
import static org.apache.aurora.scheduler.thrift.Fixtures.JOB_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.JOB_NAME;
import static org.apache.aurora.scheduler.thrift.Fixtures.NOT_ENOUGH_QUOTA;
import static org.apache.aurora.scheduler.thrift.Fixtures.ROLE;
import static org.apache.aurora.scheduler.thrift.Fixtures.TASK_ID;
import static org.apache.aurora.scheduler.thrift.Fixtures.UPDATE_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.USER;
import static org.apache.aurora.scheduler.thrift.Fixtures.UU_ID;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertOkResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.defaultTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeJob;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeProdJob;
import static org.apache.aurora.scheduler.thrift.Fixtures.nonProductionTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.okResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.productionTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.response;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.ADD_INSTANCES;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.CREATE_JOB;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.CREATE_OR_UPDATE_CRON;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.DRAIN_HOSTS;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.END_MAINTENANCE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.KILL_TASKS;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.MAINTENANCE_STATUS;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.NOOP_JOB_UPDATE_MESSAGE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.NO_CRON;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.RESTART_SHARDS;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.START_JOB_UPDATE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.START_MAINTENANCE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.jobAlreadyExistsMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.noCronScheduleMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.notScheduledCronMessage;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchedulerThriftInterfaceTest extends EasyMockTest {

  private static final String AUDIT_MESSAGE = "message";
  private static final AuditData AUDIT = new AuditData(USER, Optional.of(AUDIT_MESSAGE));
  private static final Thresholds THRESHOLDS = new Thresholds(1000, 2000);
  private static final String EXECUTOR_NAME = Api_Constants.AURORA_EXECUTOR_NAME;
  private static final ImmutableSet<Metadata> METADATA = ImmutableSet.of(
      Metadata.builder().setKey("k1").setValue("v1").build(),
      Metadata.builder().setKey("k2").setValue("v2").build());

  private StorageTestUtil storageUtil;
  private StorageBackup backup;
  private Recovery recovery;
  private MaintenanceController maintenance;
  private AuroraAdmin.Iface thrift;
  private CronJobManager cronJobManager;
  private QuotaManager quotaManager;
  private StateManager stateManager;
  private UUIDGenerator uuidGenerator;
  private JobUpdateController jobUpdateController;
  private ReadOnlyScheduler.Iface readOnlyScheduler;
  private AuditMessages auditMessages;
  private TaskReconciler taskReconciler;
  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    backup = createMock(StorageBackup.class);
    recovery = createMock(Recovery.class);
    maintenance = createMock(MaintenanceController.class);
    cronJobManager = createMock(CronJobManager.class);
    quotaManager = createMock(QuotaManager.class);
    stateManager = createMock(StateManager.class);
    uuidGenerator = createMock(UUIDGenerator.class);
    jobUpdateController = createMock(JobUpdateController.class);
    readOnlyScheduler = createMock(ReadOnlyScheduler.Iface.class);
    auditMessages = createMock(AuditMessages.class);
    taskReconciler = createMock(TaskReconciler.class);
    statsProvider = new FakeStatsProvider();

    thrift = getResponseProxy(
        new SchedulerThriftInterface(
            TaskTestUtil.CONFIGURATION_MANAGER,
            THRESHOLDS,
            storageUtil.storage,
            backup,
            recovery,
            cronJobManager,
            maintenance,
            quotaManager,
            stateManager,
            uuidGenerator,
            jobUpdateController,
            readOnlyScheduler,
            auditMessages,
            taskReconciler,
            statsProvider));
  }

  private static AuroraAdmin.Iface getResponseProxy(AuroraAdmin.Iface realThrift) {
    // Capture all API method calls to validate response objects.
    Class<AuroraAdmin.Iface> thriftClass = AuroraAdmin.Iface.class;
    return (AuroraAdmin.Iface) Proxy.newProxyInstance(
        thriftClass.getClassLoader(),
        new Class<?>[] {thriftClass},
        (o, method, args) -> {
          Response response;
          try {
            response = (Response) method.invoke(realThrift, args);
          } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getTargetException());
          }
          assertTrue(response.hasResponseCode());
          assertNotNull(response.getDetails());
          return response;
        });
  }

  private static SanitizedConfiguration fromUnsanitized(JobConfiguration job)
      throws TaskDescriptionException {

    return SanitizedConfiguration.fromUnsanitized(TaskTestUtil.CONFIGURATION_MANAGER, job);
  }

  @Test
  public void testCreateJobNoLock() throws Exception {
    JobConfiguration job = makeProdJob();
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(sanitized, ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getJobConfig().getTaskConfig(),
        sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(makeProdJob()));
    assertEquals(1L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobWithLock() throws Exception {
    JobConfiguration job = makeProdJob();
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(sanitized, ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getJobConfig().getTaskConfig(),
        sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(job));
    assertEquals(1L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsForCron() throws Exception {
    JobConfiguration job = makeProdJob().mutate().setCronSchedule("").build();

    control.replay();

    assertEquals(invalidResponse(NO_CRON), thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsConfigCheck() throws Exception {
    JobConfiguration job = makeJob(INVALID_TASK_CONFIG);
    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsLockCheck() throws Exception {
    JobConfiguration job = makeJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("Job is updating"));

    control.replay();

    assertResponse(JOB_UPDATING_ERROR, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsJobExists() throws Exception {
    JobConfiguration job = makeJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), buildScheduledTask());

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsCronJobExists() throws Exception {
    JobConfiguration job = makeJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectCronJob();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsInstanceCheck() throws Exception {
    JobConfiguration job = makeJob(defaultTask(true), THRESHOLDS.getMaxTasksPerJob() + 1);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expect(quotaManager.checkInstanceAddition(
        anyObject(TaskConfig.class),
        anyInt(),
        eq(storageUtil.mutableStoreProvider))).andReturn(ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsQuotaCheck() throws Exception {
    JobConfiguration job = makeProdJob();
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(sanitized, NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  private void assertMessageMatches(Response response, String string) {
    // TODO(wfarner): This test coverage could be much better.  Circle back to apply more thorough
    // response contents testing throughout.
    assertTrue(Iterables.any(response.getDetails(), detail -> detail.getMessage().equals(string)));
  }

  @Test
  public void testCreateEmptyJob() throws Exception {
    control.replay();

    JobConfiguration job = JobConfiguration.builder().setKey(JOB_KEY).setOwner(IDENTITY).build();
    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsNoExecutorOrContainerConfig() throws Exception {
    JobConfiguration job = makeJob()
        .mutate()
        .setTaskConfig(makeJob().getTaskConfig().mutate().clearExecutorConfig().build())
        .build();

    control.replay();

    Response response = thrift.createJob(job);
    assertResponse(INVALID_REQUEST, response);
    assertMessageMatches(response, ConfigurationManager.NO_EXECUTOR_OR_CONTAINER);
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateHomogeneousJobNoInstances() throws Exception {
    JobConfiguration job = makeJob()
        .mutate()
        .setInstanceCount(0)
        .build();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobNegativeInstanceCount() throws Exception {
    JobConfiguration job = makeJob()
        .mutate()
        .setInstanceCount(-1)
        .build();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobNoResources() throws Exception {
    control.replay();

    TaskConfig task = productionTask()
        .mutate()
        .clearResources()
        .build();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobBadCpu() throws Exception {
    control.replay();

    TaskConfig task = productionTask()
        .mutate()
        .setResources(ImmutableSet.of(
            withNumCpus(0.0),
            withRamMb(1024),
            withDiskMb(1024)))
        .build();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobBadRam() throws Exception {
    control.replay();

    TaskConfig task = productionTask()
        .mutate()
        .setResources(ImmutableSet.of(
            withNumCpus(1),
            withRamMb(-123),
            withDiskMb(1024)))
        .build();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobBadDisk() throws Exception {
    control.replay();

    TaskConfig task = productionTask()
        .mutate()
        .setResources(ImmutableSet.of(
            withNumCpus(1),
            withRamMb(1024),
            withDiskMb(0)))
        .build();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
  }

  @Test
  public void testCreateJobGoodResources() throws Exception {

    TaskConfig task = productionTask()
        .mutate()
        .setResources(ImmutableSet.of(
            withNumCpus(1.0),
            withRamMb(1024),
            withDiskMb(1024)))
        .build();

    JobConfiguration job = makeJob(task);
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(sanitized, ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
            storageUtil.mutableStoreProvider,
            sanitized.getJobConfig().getTaskConfig(),
            sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(job));
  }

  @Test
  public void testCreateJobPopulateDefaults() throws Exception {
    Set<Resource> resources = ImmutableSet.of(withNumCpus(1.0), withRamMb(1024), withDiskMb(1024));
    TaskConfig task = TaskConfig.builder()
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(ExecutorConfig.builder().setName(EXECUTOR_NAME).setData("config"))
        .setResources(resources)
        .setIsService(true)
        .setProduction(true)
        .setTier(TaskTestUtil.PROD_TIER_NAME)
        .setOwner(IDENTITY)
        .setContainer(Container.withMesos(MesosContainer.builder()))
        .setJob(JOB_KEY)
        .build();
    JobConfiguration job = makeJob(task);

    JobConfiguration._Builder sanitized = job.mutate();
    sanitized.mutableTaskConfig()
        .setJob(JOB_KEY)
        .setPriority(0)
        .setIsService(true)
        .setProduction(true)
        .setTaskLinks(ImmutableMap.of())
        .setConstraints(ImmutableSet.of())
        .setMaxTaskFailures(0)
        .setResources(resources);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(sanitized.getTaskConfig(), ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getTaskConfig(),
        ImmutableSet.of(0));

    control.replay();

    assertOkResponse(thrift.createJob(job));
    assertEquals(1L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateUnauthorizedDedicatedJob() throws Exception {
    control.replay();

    TaskConfig._Builder task = nonProductionTask().mutate();
    task.setConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos"))));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testLimitConstraintForDedicatedJob() throws Exception {
    control.replay();

    TaskConfig._Builder task = nonProductionTask().mutate();
    task.setConstraints(ImmutableSet.of(dedicatedConstraint(1)));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testMultipleValueConstraintForDedicatedJob() throws Exception {
    control.replay();

    TaskConfig._Builder task = nonProductionTask().mutate();
    task.setConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos", "test"))));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  private ScheduledTask buildTaskForJobUpdate(int instanceId) {
    return buildTaskForJobUpdate(instanceId, "data");
  }

  private ScheduledTask buildTaskForJobUpdate(int instanceId, String executorData) {
    return ScheduledTask.builder()
        .setAssignedTask(AssignedTask.builder()
            .setInstanceId(instanceId)
            .setTask(populatedTask()
                .mutate()
                .setIsService(true)
                .setExecutorConfig(ExecutorConfig.builder().setName(EXECUTOR_NAME)
                    .setData(executorData))))
        .build();
  }

  private ScheduledTask buildScheduledTask() {
    return buildScheduledTask(JOB_NAME, TASK_ID);
  }

  private static ScheduledTask buildScheduledTask(String jobName, String taskId) {
    return ScheduledTask.builder()
        .setAssignedTask(AssignedTask.builder()
            .setTaskId(taskId)
            .setInstanceId(0)
            .setTask(TaskConfig.builder()
                .setJob(JOB_KEY.mutate().setName(jobName))
                .setOwner(IDENTITY)))
        .build();
  }

  private void expectTransitionsToKilling() {
    expectTransitionsToKilling(Optional.absent());
  }

  private void expectTransitionsToKilling(Optional<String> message) {
    expect(auditMessages.killedByRemoteUser(message)).andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.KILLING,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);
  }

  @Test
  public void testJobScopedKillsActive() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(JOB_KEY, null, null));
    assertEquals(1L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testInstanceScoped() throws Exception {
    Query.Builder query = Query.instanceScoped(JOB_KEY, ImmutableSet.of(1)).active();
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(JOB_KEY, ImmutableSet.of(1), null));
    assertEquals(1L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testKillTasksLockCheckFailed() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    ScheduledTask task2 = buildScheduledTask("job_bar", TASK_ID);
    storageUtil.expectTaskFetch(query, buildScheduledTask(), task2);
    jobUpdateController.assertNotUpdating(JOB_KEY);
    jobUpdateController.assertNotUpdating(task2.getAssignedTask().getTask().getJob());
    expectLastCall().andThrow(new JobUpdatingException("Job is updating"));

    control.replay();

    assertResponse(
        JOB_UPDATING_ERROR,
        thrift.killTasks(JOB_KEY, null, null));
    assertEquals(0L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testKillNonExistentTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    storageUtil.expectTaskFetch(query);

    control.replay();

    Response response = thrift.killTasks(JOB_KEY, null, null);
    assertOkResponse(response);
    assertMessageMatches(response, SchedulerThriftInterface.NO_TASKS_TO_KILL_MESSAGE);
    assertEquals(0L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testKillTasksWithMessage() throws Exception {
    String message = "Test message";
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectTransitionsToKilling(Optional.of(message));

    control.replay();

    assertOkResponse(thrift.killTasks(JOB_KEY, null, message));
  }

  @Test
  public void testPruneTasksRejectsActiveStates() throws Exception {
    control.replay();
    Response rsp = thrift.pruneTasks(TaskQuery.builder().setStatuses(Tasks.ACTIVE_STATES).build());
    assertResponse(ResponseCode.ERROR, rsp);
  }

  @Test
  public void testPruneTasksRejectsMixedStates() throws Exception {
    control.replay();
    Response rsp = thrift.pruneTasks(TaskQuery.builder()
        .setStatuses(ImmutableSet.of(ScheduleStatus.FINISHED, ScheduleStatus.KILLING))
        .build());
    assertResponse(ResponseCode.ERROR, rsp);
  }

  @Test
  public void testPruneTasksAddsDefaultStatuses() throws Exception {
    storageUtil.expectTaskFetch(
        Query.arbitrary(TaskQuery.builder().setStatuses(Tasks.TERMINAL_STATES).build()),
        buildScheduledTask());
    stateManager.deleteTasks(
        storageUtil.mutableStoreProvider,
        ImmutableSet.of(buildScheduledTask().getAssignedTask().getTaskId()));
    control.replay();

    assertOkResponse(thrift.pruneTasks(TaskQuery.builder().build()));
  }

  @Test
  public void testPruneTasksRespectsScopedTerminalState() throws Exception {
    storageUtil.expectTaskFetch(
        Query.arbitrary(
            TaskQuery.builder().setStatuses(ImmutableSet.of(ScheduleStatus.FAILED)).build()),
        buildScheduledTask());
    stateManager.deleteTasks(
        storageUtil.mutableStoreProvider,
        ImmutableSet.of(buildScheduledTask().getAssignedTask().getTaskId()));
    control.replay();

    assertOkResponse(thrift.pruneTasks(
        TaskQuery.builder().setStatuses(ImmutableSet.of(ScheduleStatus.FAILED)).build()));
  }

  @Test
  public void testPruneTasksAppliesQueryLimit() throws Exception {
    TaskQuery query = TaskQuery.builder().setLimit(3).build();
    storageUtil.expectTaskFetch(
        Query.arbitrary(query.mutate().setStatuses(Tasks.TERMINAL_STATES).build()),
        buildScheduledTask("a/b/c", "task1"),
        buildScheduledTask("a/b/c", "task2"),
        buildScheduledTask("a/b/c", "task3"),
        buildScheduledTask("a/b/c", "task4"),
        buildScheduledTask("a/b/c", "task5"));
    stateManager.deleteTasks(
        storageUtil.mutableStoreProvider,
        ImmutableSet.of("task1", "task2", "task3"));
    control.replay();

    assertOkResponse(thrift.pruneTasks(query));
  }

  @Test
  public void testSetQuota() throws Exception {
    ResourceAggregate resourceAggregate = ResourceAggregate.builder()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200)
        .build();
    quotaManager.saveQuota(
        ROLE,
        resourceAggregate.mutate()
            .setResources(ImmutableSet.of(withNumCpus(10), withRamMb(200), withDiskMb(100)))
            .build(),
        storageUtil.mutableStoreProvider);

    control.replay();

    assertOkResponse(thrift.setQuota(ROLE, resourceAggregate));
  }

  @Test
  public void testSetQuotaFails() throws Exception {
    ResourceAggregate resourceAggregate = ResourceAggregate.builder()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200)
        .build();
    quotaManager.saveQuota(
        ROLE,
        resourceAggregate.mutate()
            .setResources(ImmutableSet.of(withNumCpus(10), withRamMb(200), withDiskMb(100)))
            .build(),
        storageUtil.mutableStoreProvider);

    expectLastCall().andThrow(new QuotaManager.QuotaException("fail"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.setQuota(ROLE, resourceAggregate));
  }

  @Test
  public void testForceTaskState() throws Exception {
    ScheduleStatus status = ScheduleStatus.FAILED;

    expect(auditMessages.transitionedBy()).andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.FAILED,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);

    control.replay();

    assertOkResponse(thrift.forceTaskState(TASK_ID, status));
  }

  @Test
  public void testBackupControls() throws Exception {
    backup.backupNow();

    Set<String> backups = ImmutableSet.of("a", "b");
    expect(recovery.listBackups()).andReturn(backups);

    String backupId = "backup";
    recovery.stage(backupId);

    Query.Builder query = Query.taskScoped("taskId");
    Set<ScheduledTask> queryResult = ImmutableSet.of(
        ScheduledTask.builder().setStatus(ScheduleStatus.RUNNING).build());
    expect(recovery.query(query)).andReturn(queryResult);

    recovery.deleteTasks(query);

    recovery.commit();

    recovery.unload();

    control.replay();

    assertEquals(okEmptyResponse(), thrift.performBackup());

    assertEquals(
        okResponse(Result.withListBackupsResult(ListBackupsResult.builder().setBackups(backups))),
        thrift.listBackups());

    assertEquals(okEmptyResponse(), thrift.stageRecovery(backupId));

    assertEquals(
        okResponse(Result.withQueryRecoveryResult(
            QueryRecoveryResult.builder().setTasks(queryResult))),
        thrift.queryRecovery(query.get()));

    assertEquals(okEmptyResponse(), thrift.deleteRecoveryTasks(query.get()));

    assertEquals(okEmptyResponse(), thrift.commitRecovery());

    assertEquals(okEmptyResponse(), thrift.unloadRecovery());
  }

  @Test
  public void testRecoveryException() throws Exception {
    Throwable recoveryException = new RecoveryException("Injected");

    String backupId = "backup";
    recovery.stage(backupId);
    expectLastCall().andThrow(recoveryException);

    control.replay();

    try {
      thrift.stageRecovery(backupId);
      fail("No recovery exception thrown.");
    } catch (RecoveryException e) {
      assertEquals(recoveryException.getMessage(), e.getMessage());
    }
  }

  @Test
  public void testRestartShards() throws Exception {
    Set<Integer> shards = ImmutableSet.of(0);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(
        Query.instanceScoped(JOB_KEY, shards).active(),
        buildScheduledTask());

    expect(auditMessages.restartedByRemoteUser())
        .andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.RESTARTING,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);

    control.replay();

    assertOkResponse(
        thrift.restartShards(JOB_KEY, shards));
    assertEquals(1L, statsProvider.getLongValue(RESTART_SHARDS));
  }

  @Test
  public void testRestartShardsLockCheckFails() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));

    control.replay();

    assertResponse(
        JOB_UPDATING_ERROR,
        thrift.restartShards(JOB_KEY, shards));
    assertEquals(0L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testRestartShardsNotFoundTasksFailure() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.instanceScoped(JOB_KEY, shards).active());

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.restartShards(JOB_KEY, shards));
    assertEquals(0L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testReplaceCronTemplate() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);
    cronJobManager.updateJob(anyObject(SanitizedCronJob.class));
    control.replay();

    assertOkResponse(thrift.replaceCronTemplate(CRON_JOB));
    assertEquals(1L, statsProvider.getLongValue(CREATE_OR_UPDATE_CRON));
  }

  @Test
  public void testReplaceCronTemplateFailedLockValidation() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));
    control.replay();

    assertResponse(JOB_UPDATING_ERROR, thrift.replaceCronTemplate(CRON_JOB));
    assertEquals(0L, statsProvider.getLongValue(CREATE_OR_UPDATE_CRON));
  }

  @Test
  public void testReplaceCronTemplateDoesNotExist() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);
    cronJobManager.updateJob(anyObject(SanitizedCronJob.class));
    expectLastCall().andThrow(new CronException("Nope"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.replaceCronTemplate(CRON_JOB));
    assertEquals(0L, statsProvider.getLongValue(CREATE_OR_UPDATE_CRON));
  }

  @Test
  public void testStartCronJob() throws Exception {
    cronJobManager.startJobNow(JOB_KEY);
    control.replay();
    assertResponse(OK, thrift.startCronJob(JOB_KEY));
  }

  @Test
  public void testStartCronJobFailsInCronManager() throws Exception {
    cronJobManager.startJobNow(JOB_KEY);
    expectLastCall().andThrow(new CronException("failed"));
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startCronJob(JOB_KEY));
  }

  @Test
  public void testScheduleCronCreatesJob() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectNoCronJob().times(2);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    cronJobManager.createJob(SanitizedCronJob.from(sanitized));
    control.replay();
    assertResponse(OK, thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testScheduleCronFailsCreationDueToExistingNonCron() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectNoCronJob();
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), buildScheduledTask());
    control.replay();
    assertEquals(
        invalidResponse(jobAlreadyExistsMessage(JOB_KEY)),
        thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testScheduleCronUpdatesJob() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectCronJob();
    cronJobManager.updateJob(SanitizedCronJob.from(sanitized));
    control.replay();

    assertResponse(OK, thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testScheduleCronJobFailedTaskConfigValidation() throws Exception {
    control.replay();
    JobConfiguration job = makeJob(INVALID_TASK_CONFIG);
    assertResponse(INVALID_REQUEST, thrift.scheduleCronJob(job));
  }

  @Test
  public void testScheduleCronJobFailsLockValidation() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));
    control.replay();
    assertResponse(JOB_UPDATING_ERROR, thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testScheduleCronJobFailsWithNoCronSchedule() throws Exception {
    control.replay();

    assertEquals(
        invalidResponse(noCronScheduleMessage(JOB_KEY)),
        thrift.scheduleCronJob(makeJob()));
  }

  @Test
  public void testScheduleCronFailsQuotaCheck() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(CRON_JOB);

    expectCronQuotaCheck(sanitized.getJobConfig(), NOT_ENOUGH_QUOTA);

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testDescheduleCronJob() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expect(cronJobManager.deleteJob(JOB_KEY)).andReturn(true);

    control.replay();

    assertResponse(OK, thrift.descheduleCronJob(CRON_JOB.getKey()));
  }

  @Test
  public void testDescheduleCronJobFailsLockValidation() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));
    control.replay();
    assertResponse(JOB_UPDATING_ERROR, thrift.descheduleCronJob(CRON_JOB.getKey()));
  }

  @Test
  public void testDescheduleNotACron() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expect(cronJobManager.deleteJob(JOB_KEY)).andReturn(false);
    control.replay();

    assertEquals(
        okEmptyResponse(notScheduledCronMessage(JOB_KEY)),
        thrift.descheduleCronJob(JOB_KEY));
  }

  @Test
  public void testUnauthorizedDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask().mutate()
        .setConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos"))))
        .build();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  private static Set<HostStatus> status(String host, MaintenanceMode mode) {
    return ImmutableSet.of(HostStatus.builder().setHost(host).setMode(mode).build());
  }

  @Test
  public void testHostMaintenance() throws Exception {
    Set<String> hostnames = ImmutableSet.of("a");
    Set<HostStatus> none = status("a", NONE);
    Set<HostStatus> scheduled = status("a", SCHEDULED);
    Set<HostStatus> draining = status("a", DRAINING);
    Set<HostStatus> drained = status("a", DRAINING);
    expect(maintenance.getStatus(hostnames)).andReturn(none);
    expect(maintenance.startMaintenance(hostnames)).andReturn(scheduled);
    expect(maintenance.drain(hostnames)).andReturn(draining);
    expect(maintenance.getStatus(hostnames)).andReturn(draining);
    expect(maintenance.getStatus(hostnames)).andReturn(drained);
    expect(maintenance.endMaintenance(hostnames)).andReturn(none);

    control.replay();

    Hosts hosts = Hosts.builder().setHostNames(hostnames).build();

    assertEquals(
        none,
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(1L, statsProvider.getLongValue(MAINTENANCE_STATUS));
    assertEquals(
        scheduled,
        thrift.startMaintenance(hosts).getResult().getStartMaintenanceResult()
            .getStatuses());
    assertEquals(1L, statsProvider.getLongValue(START_MAINTENANCE));
    assertEquals(
        draining,
        thrift.drainHosts(hosts).getResult().getDrainHostsResult().getStatuses());
    assertEquals(1L, statsProvider.getLongValue(DRAIN_HOSTS));
    assertEquals(
        draining,
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(2L, statsProvider.getLongValue(MAINTENANCE_STATUS));
    assertEquals(
        drained,
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(3L, statsProvider.getLongValue(MAINTENANCE_STATUS));
    assertEquals(
        none,
        thrift.endMaintenance(hosts).getResult().getEndMaintenanceResult().getStatuses());
    assertEquals(1L, statsProvider.getLongValue(END_MAINTENANCE));
  }

  private static Response okEmptyResponse() {
    return response(OK, Optional.absent());
  }

  private static Response okEmptyResponse(String message) {
    return Responses.empty()
        .setResponseCode(OK)
        .setDetails(ImmutableList.of(ResponseDetail.builder().setMessage(message).build()))
        .build();
  }

  private static Response invalidResponse(String message) {
    return Responses.empty()
        .setResponseCode(INVALID_REQUEST)
        .setDetails(ImmutableList.of(ResponseDetail.builder().setMessage(message).build()))
        .build();
  }

  @Test
  public void testSnapshot() throws Exception {
    storageUtil.storage.snapshot();
    expectLastCall();

    storageUtil.storage.snapshot();
    expectLastCall().andThrow(new StorageException("mock error!"));

    control.replay();

    assertOkResponse(thrift.snapshot());

    try {
      thrift.snapshot();
      fail("No StorageException thrown.");
    } catch (StorageException e) {
      // Expected.
    }
  }

  @Test
  public void testExplicitTaskReconciliationWithNoBatchSize() throws Exception {
    ExplicitReconciliationSettings settings = ExplicitReconciliationSettings.builder().build();

    taskReconciler.triggerExplicitReconciliation(Optional.absent());
    expectLastCall();

    control.replay();

    assertOkResponse(thrift.triggerExplicitTaskReconciliation(settings));
  }

  @Test
  public void testExplicitTaskReconciliationWithValidBatchSize() throws Exception {
    ExplicitReconciliationSettings settings = ExplicitReconciliationSettings.builder()
        .setBatchSize(10)
        .build();

    taskReconciler.triggerExplicitReconciliation(Optional.of(settings.getBatchSize()));
    expectLastCall();

    control.replay();
    assertOkResponse(thrift.triggerExplicitTaskReconciliation(settings));
  }

  @Test
  public void testExplicitTaskReconciliationWithNegativeBatchSize() throws Exception {
    ExplicitReconciliationSettings settings = ExplicitReconciliationSettings.builder()
        .setBatchSize(-1000)
        .build();

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.triggerExplicitTaskReconciliation(settings));
  }

  @Test
  public void testExplicitTaskReconciliationWithZeroBatchSize() throws Exception {
    ExplicitReconciliationSettings settings = ExplicitReconciliationSettings.builder()
        .setBatchSize(0)
        .build();

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.triggerExplicitTaskReconciliation(settings));
  }

  @Test
  public void testImplicitTaskReconciliation() throws Exception {
    taskReconciler.triggerImplicitReconciliation();
    expectLastCall();

    control.replay();

    assertOkResponse(thrift.triggerImplicitTaskReconciliation());
  }

  @Test
  public void testAddInstancesWithInstanceKey() throws Exception {
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    ScheduledTask activeTask = buildScheduledTask();
    TaskConfig task = activeTask.getAssignedTask().getTask();
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), activeTask);
    expect(quotaManager.checkInstanceAddition(
        task,
        2,
        storageUtil.mutableStoreProvider)).andReturn(ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        task,
        ImmutableSet.of(1, 2));

    control.replay();

    assertOkResponse(thrift.addInstances(INSTANCE_KEY, 2));
    assertEquals(2L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesWithInstanceKeyFailsWithNoInstance() throws Exception {
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());

    control.replay();

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_INSTANCE_ID),
        thrift.addInstances(INSTANCE_KEY, 2));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesWithInstanceKeyFailsInvalidCount() throws Exception {
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());

    control.replay();

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_INSTANCE_COUNT),
        thrift.addInstances(INSTANCE_KEY, 0));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesFailsCronJob() throws Exception {
    expectCronJob();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(INSTANCE_KEY, 1));
  }

  @Test(expected = StorageException.class)
  public void testAddInstancesFailsWithNonTransient() throws Exception {
    expect(storageUtil.jobStore.fetchJob(JOB_KEY)).andThrow(new StorageException("no retry"));

    control.replay();

    thrift.addInstances(INSTANCE_KEY, 1);
  }

  @Test
  public void testAddInstancesLockCheckFails() throws Exception {
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));

    control.replay();

    assertResponse(JOB_UPDATING_ERROR, thrift.addInstances(INSTANCE_KEY, 1));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesFailsQuotaCheck() throws Exception {
    ScheduledTask activeTask = buildScheduledTask();
    TaskConfig task = activeTask.getAssignedTask().getTask();
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), activeTask);
    expectInstanceQuotaCheck(task, NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(INSTANCE_KEY, 1));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesInstanceCollisionFailure() throws Exception {
    ScheduledTask activeTask = buildScheduledTask();
    TaskConfig task = activeTask.getAssignedTask().getTask();
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), activeTask);
    expectInstanceQuotaCheck(task, ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        task,
        ImmutableSet.of(1));
    expectLastCall().andThrow(new IllegalArgumentException("instance collision"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(INSTANCE_KEY, 1));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testStartUpdate() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    TaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();

    ScheduledTask oldTask1 = buildTaskForJobUpdate(0, "old");
    ScheduledTask oldTask2 = buildTaskForJobUpdate(1, "old");
    ScheduledTask oldTask3 = buildTaskForJobUpdate(2, "old2");
    ScheduledTask oldTask4 = buildTaskForJobUpdate(3, "old2");
    ScheduledTask oldTask5 = buildTaskForJobUpdate(4, "old");
    ScheduledTask oldTask6 = buildTaskForJobUpdate(5, "old");
    ScheduledTask oldTask7 = buildTaskForJobUpdate(6, "old");

    JobUpdate update = buildJobUpdate(6, newTask, ImmutableMap.of(
        oldTask1.getAssignedTask().getTask(), ImmutableSet.of(range(0, 1), range(4, 6)),
        oldTask3.getAssignedTask().getTask(), ImmutableSet.of(range(2, 3))
    ));

    expect(quotaManager.checkJobUpdate(
        update,
        storageUtil.mutableStoreProvider)).andReturn(ENOUGH_QUOTA);

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        oldTask1,
        oldTask2,
        oldTask3,
        oldTask4,
        oldTask5,
        oldTask6,
        oldTask7);

    jobUpdateController.start(update, AUDIT);

    control.replay();

    Response response =
        assertOkResponse(thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE));
    assertEquals(
        StartJobUpdateResult.builder().setKey(UPDATE_KEY).setUpdateSummary(
        update.getSummary()), response.getResult().getStartJobUpdateResult());
    assertEquals(6L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  private void expectJobUpdateQuotaCheck(QuotaCheckResult result) {
    expect(quotaManager.checkJobUpdate(
        anyObject(JobUpdate.class),
        eq(storageUtil.mutableStoreProvider))).andReturn(result);
  }

  @Test
  public void testStartUpdateEmptyDesired() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    TaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();

    ScheduledTask oldTask1 = buildTaskForJobUpdate(0);
    ScheduledTask oldTask2 = buildTaskForJobUpdate(1);

    // Set instance count to 1 to generate empty desired state in diff.
    JobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask1.getAssignedTask().getTask(), ImmutableSet.of(range(0, 1))));

    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    // Set diff-adjusted JobUpdate expectations.
    JobUpdate._Builder expected = update.mutate();
    expected.mutableInstructions().setInitialState(ImmutableSet.of(
        InstanceTaskConfig.builder()
            .setTask(newTask)
            .setInstances(ImmutableSet.of(range(1, 1)))
            .build()));
    expected.mutableInstructions().clearDesiredState();

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        oldTask1,
        oldTask2);

    jobUpdateController.start(expected.build(), AUDIT);

    control.replay();

    Response response = assertOkResponse(
        thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE));
    assertEquals(
        StartJobUpdateResult.builder()
            .setKey(UPDATE_KEY)
            .setUpdateSummary(expected.getSummary())
            .build(),
        response.getResult().getStartJobUpdateResult());
    assertEquals(1L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test(expected = NullPointerException.class)
  public void testStartUpdateFailsNullRequest() throws Exception {
    control.replay();
    thrift.startJobUpdate(null, AUDIT_MESSAGE);
  }

  @Test(expected = NullPointerException.class)
  public void testStartUpdateFailsNullTaskConfig() throws Exception {
    control.replay();
    thrift.startJobUpdate(
        JobUpdateRequest.builder()
            .clearTaskConfig()
            .setInstanceCount(5)
            .setSettings(buildJobUpdateSettings())
            .build(),
        AUDIT_MESSAGE);
  }

  @Test
  public void testStartUpdateFailsInvalidGroupSize() throws Exception {
    control.replay();

    JobUpdateRequest._Builder updateRequest = buildServiceJobUpdateRequest().mutate();
    updateRequest.mutableSettings().setUpdateGroupSize(0);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_GROUP_SIZE),
        thrift.startJobUpdate(updateRequest.build(), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxInstanceFailures() throws Exception {
    control.replay();

    JobUpdateRequest._Builder updateRequest = buildServiceJobUpdateRequest().mutate();
    updateRequest.mutableSettings().setMaxPerInstanceFailures(-1);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MAX_INSTANCE_FAILURES),
        thrift.startJobUpdate(updateRequest.build(), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsTooManyPerInstanceFailures() throws Exception {
    control.replay();

    JobUpdateRequest._Builder updateRequest = buildServiceJobUpdateRequest().mutate();
    updateRequest.mutableSettings()
        .setMaxPerInstanceFailures(THRESHOLDS.getMaxUpdateInstanceFailures() + 10);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.TOO_MANY_POTENTIAL_FAILED_INSTANCES),
        thrift.startJobUpdate(updateRequest.build(), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxFailedInstances() throws Exception {
    control.replay();

    JobUpdateRequest._Builder updateRequest = buildServiceJobUpdateRequest().mutate();
    updateRequest.mutableSettings().setMaxFailedInstances(-1);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MAX_FAILED_INSTANCES),
        thrift.startJobUpdate(updateRequest.build(), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInvalidMinWaitInRunning() throws Exception {
    control.replay();

    JobUpdateRequest._Builder updateRequest = buildServiceJobUpdateRequest().mutate();
    updateRequest.mutableSettings().setMinWaitInInstanceRunningMs(-1);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MIN_WAIT_TO_RUNNING),
        thrift.startJobUpdate(updateRequest.build(), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsNonServiceTask() throws Exception {
    control.replay();
    JobUpdateRequest request =
        buildJobUpdateRequest(populatedTask().mutate().setIsService(false).build());
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInvalidPulseTimeout() throws Exception {
    control.replay();

    JobUpdateRequest._Builder updateRequest = buildServiceJobUpdateRequest().mutate();
    updateRequest.mutableSettings().setBlockIfNoPulsesAfterMs(-1);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_PULSE_TIMEOUT),
        thrift.startJobUpdate(updateRequest.build(), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsForCronJob() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask());
    expectCronJob();

    control.replay();
    assertEquals(invalidResponse(NO_CRON), thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsConfigValidation() throws Exception {
    JobUpdateRequest request =
        buildJobUpdateRequest(INVALID_TASK_CONFIG.mutate().setIsService(true).build());

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartNoopUpdate() throws Exception {
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    TaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();

    ScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);

    JobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(range(0, 0))));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(update);
    Response response = thrift.startJobUpdate(request, AUDIT_MESSAGE);
    assertResponse(OK, response);
    assertEquals(
        NOOP_JOB_UPDATE_MESSAGE,
        Iterables.getOnlyElement(response.getDetails()).getMessage());
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateInvalidScope() throws Exception {
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    ScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);

    TaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();
    JobUpdate._Builder builder = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(range(0, 0))))
        .mutate();
    builder.mutableInstructions().mutableSettings()
        .setUpdateOnlyTheseInstances(ImmutableSet.of(range(100, 100)));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(builder.build());
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  private static Range range(int first, int last) {
    return Range.builder().setFirst(first).setLast(last).build();
  }

  @Test
  public void testStartScopeIncludesNoop() throws Exception {
    // Test for regression of AURORA-1332: a scoped update should be allowed when unchanged
    // instances are included in the scope.

    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    ScheduledTask._Builder taskBuilder = buildTaskForJobUpdate(0).mutate();
    taskBuilder.mutableAssignedTask().mutableTask()
        .setResources(ImmutableSet.of(
                withNumCpus(100),
                withRamMb(1024),
                withDiskMb(1024)))
        .build();
    ScheduledTask newTask = taskBuilder.build();

    ScheduledTask oldTask1 = buildTaskForJobUpdate(1);
    ScheduledTask oldTask2 = buildTaskForJobUpdate(2);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        newTask, oldTask1, oldTask2);
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);
    jobUpdateController.start(EasyMock.<JobUpdate>anyObject(), eq(AUDIT));

    TaskConfig newConfig = newTask.getAssignedTask().getTask();
    JobUpdate._Builder builder = buildJobUpdate(
        3,
        newConfig,
        ImmutableMap.of(
            newConfig, ImmutableSet.of(range(0, 0)),
            oldTask1.getAssignedTask().getTask(), ImmutableSet.of(range(1, 2))))
        .mutate();
    builder.mutableInstructions().mutableSettings()
        .setUpdateOnlyTheseInstances(ImmutableSet.of(range(0, 2)));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(builder.build());
    assertResponse(OK, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(3L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInstanceCountCheck() throws Exception {
    JobUpdateRequest._Builder request = buildServiceJobUpdateRequest(populatedTask()).mutate();
    request.setInstanceCount(THRESHOLDS.getMaxTasksPerJob() + 1);
    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active());
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request.build(), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsQuotaCheck() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask());
    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    ScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);

    expectJobUpdateQuotaCheck(NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    ScheduledTask oldTask = buildTaskForJobUpdate(0, "old");
    TaskConfig newTask = buildTaskForJobUpdate(0, "new").getAssignedTask().getTask();

    JobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(range(0, 0))));

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);
    jobUpdateController.start(update, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInControllerWhenUpdateInProgress() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    ScheduledTask oldTask = buildTaskForJobUpdate(0, "old");
    TaskConfig newTask = buildTaskForJobUpdate(0, "new").getAssignedTask().getTask();

    JobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(range(0, 0))));

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);
    jobUpdateController.start(update, AUDIT);
    expectLastCall().andThrow(new UpdateInProgressException("failed", update.getSummary()));

    control.replay();

    Response response = thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE);

    assertResponse(INVALID_REQUEST, response);
    assertEquals(
        StartJobUpdateResult.builder().setKey(UPDATE_KEY)
            .setUpdateSummary(update.getSummary())
            .build(),
        response.getResult().getStartJobUpdateResult());
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testPauseJobUpdateByCoordinator() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.pauseJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testPauseJobUpdateByUser() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.pauseJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPauseMessageTooLong() throws Exception {
    expectGetRemoteUser();

    control.replay();

    assertResponse(
        OK,
        thrift.pauseJobUpdate(
            UPDATE_KEY,
            Strings.repeat("*", AuditData.MAX_MESSAGE_LENGTH + 1)));
  }

  @Test
  public void testPauseJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.pauseJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testResumeJobUpdate() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.resume(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.resumeJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testResumeJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.resume(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.resumeJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateByCoordinator() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.abortJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateByUser() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.abortJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.abortJobUpdate(UPDATE_KEY, AUDIT_MESSAGE));
  }

  @Test
  public void testPulseJobUpdatePulsedAsCoordinator() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andReturn(JobUpdatePulseStatus.OK);

    control.replay();

    assertEquals(
        okResponse(Result.withPulseJobUpdateResult(
            PulseJobUpdateResult.builder().setStatus(JobUpdatePulseStatus.OK))),
        thrift.pulseJobUpdate(UPDATE_KEY));
  }

  @Test
  public void testPulseJobUpdatePulsedAsUser() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andReturn(JobUpdatePulseStatus.OK);

    control.replay();

    assertEquals(
        okResponse(Result.withPulseJobUpdateResult(
            PulseJobUpdateResult.builder().setStatus(JobUpdatePulseStatus.OK))),
        thrift.pulseJobUpdate(UPDATE_KEY));
  }

  @Test
  public void testPulseJobUpdateFails() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andThrow(new UpdateStateException("failure"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.pulseJobUpdate(UPDATE_KEY));
  }

  @Test
  public void testGetJobUpdateSummaries() throws Exception {
    Response updateSummary = Responses.empty()
        .setResponseCode(OK)
        .setDetails(ImmutableList.of(ResponseDetail.builder().setMessage("summary").build()))
        .build();

    expect(readOnlyScheduler.getJobUpdateSummaries(
        anyObject(JobUpdateQuery.class))).andReturn(updateSummary);
    control.replay();

    assertEquals(updateSummary, thrift.getJobUpdateSummaries(JobUpdateQuery.builder().build()));
  }

  private IExpectationSetters<String> expectGetRemoteUser() {
    return expect(auditMessages.getRemoteUserName()).andReturn(USER);
  }

  private static TaskConfig populatedTask() {
    return defaultTask(true).mutate()
        .setConstraints(Sets.newHashSet(
            Constraint.builder()
                .setName("host")
                .setConstraint(TaskConstraint.withLimit(LimitConstraint.builder().setLimit(1)))
                .build()))
        .build();
  }

  private static Constraint dedicatedConstraint(int value) {
    return Constraint.builder()
        .setName(DEDICATED_ATTRIBUTE)
        .setConstraint(TaskConstraint.withLimit(LimitConstraint.builder().setLimit(value)))
        .build();
  }

  private static Constraint dedicatedConstraint(Set<String> values) {
    return Constraint.builder().setName(DEDICATED_ATTRIBUTE)
        .setConstraint(
            TaskConstraint.withValue(ValueConstraint.builder()
                .setNegated(false)
                .setValues(values)
                .build()))
        .build();
  }

  private static JobUpdateRequest buildServiceJobUpdateRequest() {
    return buildServiceJobUpdateRequest(defaultTask(true));
  }

  private static JobUpdateRequest buildServiceJobUpdateRequest(TaskConfig config) {
    return buildJobUpdateRequest(config.mutate().setIsService(true).build());
  }

  private static JobUpdateRequest buildJobUpdateRequest(TaskConfig config) {
    return JobUpdateRequest.builder()
        .setInstanceCount(6)
        .setSettings(buildJobUpdateSettings())
        .setTaskConfig(config)
        .build();
  }

  private static JobUpdateSettings buildJobUpdateSettings() {
    return JobUpdateSettings.builder()
        .setUpdateGroupSize(10)
        .setMaxFailedInstances(2)
        .setMaxPerInstanceFailures(1)
        .setMinWaitInInstanceRunningMs(15000)
        .setRollbackOnFailure(true)
        .build();
  }

  private static Integer rangesToInstanceCount(Set<Range> ranges) {
    int instanceCount = 0;
    for (Range range : ranges) {
      instanceCount += range.getLast() - range.getFirst() + 1;
    }

    return instanceCount;
  }

  private static JobUpdateRequest buildJobUpdateRequest(JobUpdate update) {
    return JobUpdateRequest.builder()
        .setInstanceCount(rangesToInstanceCount(
            update.getInstructions().getDesiredState().getInstances()))
        .setSettings(update.getInstructions().getSettings())
        .setTaskConfig(update.getInstructions().getDesiredState().getTask())
        .setMetadata(update.getSummary().getMetadata())
        .build();
  }

  private static JobUpdate buildJobUpdate(
      int instanceCount,
      TaskConfig newConfig,
      ImmutableMap<TaskConfig, ImmutableSet<Range>> oldConfigMap) {

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<TaskConfig, ImmutableSet<Range>> entry : oldConfigMap.entrySet()) {
      builder.add(InstanceTaskConfig.builder()
          .setTask(entry.getKey())
          .setInstances(entry.getValue())
          .build());
    }

    return JobUpdate.builder()
        .setSummary(JobUpdateSummary.builder()
            .setKey(UPDATE_KEY)
            .setUser(IDENTITY.getUser())
            .setMetadata(METADATA))
        .setInstructions(JobUpdateInstructions.builder()
            .setSettings(buildJobUpdateSettings())
            .setDesiredState(InstanceTaskConfig.builder()
                .setTask(newConfig)
                .setInstances(ImmutableSet.of(range(0, instanceCount - 1))))
            .setInitialState(builder.build()))
        .build();
  }

  private IExpectationSetters<?> expectCronJob() {
    return expect(storageUtil.jobStore.fetchJob(JOB_KEY))
        .andReturn(Optional.of(CRON_JOB));
  }

  private IExpectationSetters<?> expectNoCronJob() {
    return expect(storageUtil.jobStore.fetchJob(JOB_KEY))
        .andReturn(Optional.absent());
  }

  private IExpectationSetters<?> expectInstanceQuotaCheck(
      SanitizedConfiguration sanitized,
      QuotaCheckResult result) {

    return expectInstanceQuotaCheck(sanitized.getJobConfig().getTaskConfig(), result);
  }

  private IExpectationSetters<?> expectInstanceQuotaCheck(
      TaskConfig config,
      QuotaCheckResult result) {

    return expect(quotaManager.checkInstanceAddition(
        config,
        1,
        storageUtil.mutableStoreProvider)).andReturn(result);
  }

  private IExpectationSetters<?> expectCronQuotaCheck(
      JobConfiguration config,
      QuotaCheckResult result) {

    return expect(quotaManager.checkCronUpdate(config, storageUtil.mutableStoreProvider))
        .andReturn(result);
  }
}
