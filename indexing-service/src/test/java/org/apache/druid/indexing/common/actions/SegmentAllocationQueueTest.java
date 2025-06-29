/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.actions;

import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(Parameterized.class)
public class SegmentAllocationQueueTest
{
  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private SegmentAllocationQueue allocationQueue;

  private StubServiceEmitter emitter;
  private BlockingExecutorService managerExec;
  private BlockingExecutorService workerExec;

  private final boolean reduceMetadataIO;

  @Parameterized.Parameters(name = "reduceMetadataIO = {0}, useSegmentCache = {1}")
  public static Object[][] getTestParameters()
  {
    return new Object[][]{
        {true, true},
        {true, false},
        {false, true},
        {false, false}
    };
  }

  public SegmentAllocationQueueTest(boolean reduceMetadataIO, boolean useSegmentMetadataCache)
  {
    this.reduceMetadataIO = reduceMetadataIO;

    taskActionTestKit.setUseSegmentMetadataCache(useSegmentMetadataCache);
  }

  @Before
  public void setUp()
  {
    managerExec = new BlockingExecutorService("test-manager-exec");
    workerExec = new BlockingExecutorService("test-worker-exec");
    emitter = new StubServiceEmitter();

    final TaskLockConfig lockConfig = new TaskLockConfig()
    {
      @Override
      public boolean isBatchSegmentAllocation()
      {
        return true;
      }

      @Override
      public long getBatchAllocationWaitTime()
      {
        return 0;
      }

      @Override
      public boolean isBatchAllocationReduceMetadataIO()
      {
        return reduceMetadataIO;
      }

      @Override
      public int getBatchAllocationNumThreads()
      {
        return 20;
      }
    };

    allocationQueue = new SegmentAllocationQueue(
        taskActionTestKit.getTaskLockbox(),
        lockConfig,
        taskActionTestKit.getMetadataStorageCoordinator(),
        emitter,
        (corePoolSize, nameFormat) -> new WrappingScheduledExecutorService(
            nameFormat,
            nameFormat.contains("Manager") ? managerExec : workerExec,
            false
        )
    );
    allocationQueue.start();
    allocationQueue.becomeLeader();
  }

  @After
  public void tearDown()
  {
    if (allocationQueue != null) {
      allocationQueue.stop();
    }
    if (managerExec != null) {
      managerExec.shutdownNow();
    }
    emitter.flush();
  }

  @Test
  public void testBatchWithMultipleTimestamps()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .forTimestamp("2022-01-01T01:00:00")
                         .withSegmentGranularity(Granularities.DAY)
                         .withQueryGranularity(Granularities.SECOND)
                         .withLockGranularity(LockGranularity.TIME_CHUNK)
                         .withSequenceName("seq_1")
                         .build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .forTimestamp("2022-01-01T02:00:00")
                         .withSegmentGranularity(Granularities.DAY)
                         .withQueryGranularity(Granularities.SECOND)
                         .withLockGranularity(LockGranularity.TIME_CHUNK)
                         .withSequenceName("seq_2")
                         .build(),
        true
    );
  }

  @Test
  public void testBatchWithExclusiveLocks()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.EXCLUSIVE).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.EXCLUSIVE).build(),
        true
    );
  }

  @Test
  public void testBatchWithSharedLocks()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.SHARED).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.SHARED).build(),
        true
    );
  }

  @Test
  public void testBatchWithMultipleQueryGranularities()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withQueryGranularity(Granularities.SECOND).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withQueryGranularity(Granularities.MINUTE).build(),
        true
    );
  }

  @Test
  public void testMultipleDatasourcesCannotBatch()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1")).build(),
        allocateRequest().forTask(createTask(TestDataSource.KOALA, "group_1")).build(),
        false
    );
  }

  @Test
  public void testMultipleGroupIdsCannotBatch()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_2")).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_3")).build(),
        false
    );
  }

  @Test
  public void testMultipleLockGranularitiesCannotBatch()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withLockGranularity(LockGranularity.TIME_CHUNK).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withLockGranularity(LockGranularity.SEGMENT).build(),
        false
    );
  }

  @Test
  public void testMultipleAllocateIntervalsCannotBatch()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .forTimestamp("2022-01-01")
                         .withSegmentGranularity(Granularities.DAY).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .forTimestamp("2022-01-02")
                         .withSegmentGranularity(Granularities.DAY).build(),
        false
    );
  }

  @Test
  public void testConflictingPendingSegment()
  {
    SegmentAllocateRequest hourSegmentRequest =
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withSegmentGranularity(Granularities.HOUR)
                         .build();
    Future<SegmentIdWithShardSpec> hourSegmentFuture = allocationQueue.add(hourSegmentRequest);

    SegmentAllocateRequest halfHourSegmentRequest =
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withSegmentGranularity(Granularities.THIRTY_MINUTE)
                         .build();
    Future<SegmentIdWithShardSpec> halfHourSegmentFuture = allocationQueue.add(halfHourSegmentRequest);

    processDistinctDatasourceBatches();
    processDistinctDatasourceBatches();

    Assert.assertNotNull(getSegmentId(hourSegmentFuture));
    Assert.assertNull(getSegmentId(halfHourSegmentFuture));
  }

  @Test
  public void testFullAllocationQueue()
  {
    for (int i = 0; i < 2000; ++i) {
      SegmentAllocateRequest request =
          allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_" + i)).build();
      allocationQueue.add(request);
    }

    SegmentAllocateRequest request =
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "next_group")).build();
    Future<SegmentIdWithShardSpec> future = allocationQueue.add(request);

    // Verify that the future is already complete and segment allocation has failed
    Throwable t = Assert.assertThrows(ISE.class, () -> getSegmentId(future));
    Assert.assertEquals(
        "Segment allocation queue is full. Check the metric `task/action/batch/runTime` "
        + "to determine if metadata operations are slow.",
        t.getMessage()
    );
  }

  @Test
  public void testMaxBatchSize()
  {
    for (int i = 0; i < 500; ++i) {
      SegmentAllocateRequest request =
          allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1")).build();
      allocationQueue.add(request);
    }

    // Verify that next request is added to a new batch
    Assert.assertEquals(1, allocationQueue.size());
    SegmentAllocateRequest request =
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1")).build();
    allocationQueue.add(request);
    Assert.assertEquals(2, allocationQueue.size());
  }

  @Test
  public void testMultipleRequestsForSameSegment()
  {
    final List<Future<SegmentIdWithShardSpec>> segmentFutures = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      SegmentAllocateRequest request =
          allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_" + i))
                           .withSequenceName("sequence_1")
                           .withPreviousSegmentId("segment_1")
                           .build();
      segmentFutures.add(allocationQueue.add(request));
    }

    for (int i = 0; i < 10; ++i) {
      processDistinctDatasourceBatches();
    }

    SegmentIdWithShardSpec segmentId1 = getSegmentId(segmentFutures.get(0));

    for (Future<SegmentIdWithShardSpec> future : segmentFutures) {
      Assert.assertEquals(getSegmentId(future), segmentId1);
    }

    // Verify each datasource batch is marked skipped just once
    emitter.verifySum("task/action/batch/skipped", 9);
  }

  @Test
  public void testMaxWaitTime()
  {
    // Verify that the batch is due yet
  }

  @Test
  public void testRequestsFailOnLeaderChange()
  {
    final List<Future<SegmentIdWithShardSpec>> segmentFutures = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      SegmentAllocateRequest request =
          allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_" + i)).build();
      segmentFutures.add(allocationQueue.add(request));
    }

    allocationQueue.stopBeingLeader();
    processDistinctDatasourceBatches();

    for (Future<SegmentIdWithShardSpec> future : segmentFutures) {
      Throwable t = Assert.assertThrows(ISE.class, () -> getSegmentId(future));
      Assert.assertEquals("Not leader anymore", t.getMessage());
    }
  }

  private void verifyAllocationWithBatching(
      SegmentAllocateRequest a,
      SegmentAllocateRequest b,
      boolean canBatch
  )
  {
    Assert.assertEquals(0, allocationQueue.size());
    final Future<SegmentIdWithShardSpec> futureA = allocationQueue.add(a);
    final Future<SegmentIdWithShardSpec> futureB = allocationQueue.add(b);

    final int expectedCount = canBatch ? 1 : 2;
    Assert.assertEquals(expectedCount, allocationQueue.size());

    // Process both the jobs
    processDistinctDatasourceBatches();
    processDistinctDatasourceBatches();
    emitter.verifyEmitted("task/action/batch/size", expectedCount);
    emitter.verifySum("task/action/batch/submitted", expectedCount);

    Assert.assertNotNull(getSegmentId(futureA));
    Assert.assertNotNull(getSegmentId(futureB));
  }

  private SegmentIdWithShardSpec getSegmentId(Future<SegmentIdWithShardSpec> future)
  {
    try {
      return future.get(5, TimeUnit.SECONDS);
    }
    catch (ExecutionException e) {
      throw new ISE(e.getCause().getMessage());
    }
    catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private SegmentAllocateActionBuilder allocateRequest()
  {
    return new SegmentAllocateActionBuilder()
        .forDatasource(TestDataSource.WIKI)
        .forTimestamp("2022-01-01")
        .withLockGranularity(LockGranularity.TIME_CHUNK)
        .withTaskLockType(TaskLockType.SHARED)
        .withQueryGranularity(Granularities.SECOND)
        .withSegmentGranularity(Granularities.HOUR);
  }

  private Task createTask(String datasource, String groupId)
  {
    Task task = new NoopTask(null, groupId, datasource, 0, 0, null);
    taskActionTestKit.getTaskLockbox().add(task);
    return task;
  }

  /**
   * Triggers the {@link #managerExec} and the {@link #workerExec} to process a
   * queued set of jobs (i.e. segment allocation batches) for distinct datasources.
   * A single invocation of this method can process up to 1 batch for any given
   * datasource and up to {@code batchAllocationNumThreads} batches total.
   */
  private void processDistinctDatasourceBatches()
  {
    managerExec.finishNextPendingTask();
    workerExec.finishAllPendingTasks();
  }
}
