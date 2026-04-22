# Bounded Stream Supervisor Design

## Overview

This design adds support for bounded (one-time) stream processing to Druid supervisors by allowing explicit `startSequenceNumbers` and `endSequenceNumbers` configuration. This enables use cases like backfill processing, historical reprocessing, and the `resetOffsetsAndBackfill` operation.

## Key Insight

**Tasks already auto-terminate when reaching end offsets** - The `SeekableStreamIndexTaskRunner` checks `isMoreToReadAfterReadingRecord()` for each record and stops reading when all partitions reach their configured `endSequenceNumbers`. This means we don't need special completion logic at the task level - we just need to configure tasks properly and prevent the supervisor from recreating them.

## Core Design Principles

- **Optional & Backward Compatible**: Existing supervisors work unchanged
- **Explicit Intent**: Bounded mode is explicitly configured via `BoundedStreamConfig`
- **Prevent Task Recreation**: Override partition discovery to avoid infinite task creation
- **Auto-Completion**: Bounded supervisors can auto-terminate when all tasks finish
- **Clean Separation**: Bounded logic isolated from normal streaming logic

---

## 1. Configuration

### New `BoundedStreamConfig` Class

```java
package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 * Configuration for bounded (one-time) stream processing with explicit start/end offsets.
 * 
 * When configured, the supervisor will:
 * 1. Create tasks starting at the specified startSequenceNumbers
 * 2. Tasks will automatically stop when they reach endSequenceNumbers
 * 3. Supervisor will not recreate tasks after they complete
 * 4. Optionally auto-terminate when all tasks are done
 * 
 * This is useful for:
 * - Backfill processing (via resetOffsetsAndBackfill)
 * - Historical reprocessing
 * - One-time migration tasks
 */
public class BoundedStreamConfig
{
  private final Map<?, ?> startSequenceNumbers;  // Partition -> Start Offset
  private final Map<?, ?> endSequenceNumbers;    // Partition -> End Offset
  private final boolean terminateOnCompletion;    // Default: true
  private final Integer taskCount;                // Optional override for bounded mode
  
  @JsonCreator
  public BoundedStreamConfig(
      @JsonProperty("startSequenceNumbers") Map<?, ?> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers") Map<?, ?> endSequenceNumbers,
      @JsonProperty("terminateOnCompletion") @Nullable Boolean terminateOnCompletion,
      @JsonProperty("taskCount") @Nullable Integer taskCount
  )
  {
    this.startSequenceNumbers = Preconditions.checkNotNull(startSequenceNumbers, "startSequenceNumbers");
    this.endSequenceNumbers = Preconditions.checkNotNull(endSequenceNumbers, "endSequenceNumbers");
    this.terminateOnCompletion = terminateOnCompletion != null ? terminateOnCompletion : true;
    this.taskCount = taskCount;
    
    // Validation
    Preconditions.checkArgument(
        !startSequenceNumbers.isEmpty(),
        "startSequenceNumbers cannot be empty"
    );
    
    Preconditions.checkArgument(
        startSequenceNumbers.keySet().equals(endSequenceNumbers.keySet()),
        "startSequenceNumbers and endSequenceNumbers must have matching partition sets. Start: %s, End: %s",
        startSequenceNumbers.keySet(),
        endSequenceNumbers.keySet()
    );
  }
  
  @JsonProperty
  public Map<?, ?> getStartSequenceNumbers()
  {
    return startSequenceNumbers;
  }
  
  @JsonProperty
  public Map<?, ?> getEndSequenceNumbers()
  {
    return endSequenceNumbers;
  }
  
  @JsonProperty
  public boolean isTerminateOnCompletion()
  {
    return terminateOnCompletion;
  }
  
  @Nullable
  @JsonProperty
  public Integer getTaskCount()
  {
    return taskCount;
  }
}
```

### Modification to `SeekableStreamSupervisorIOConfig`

```java
public abstract class SeekableStreamSupervisorIOConfig
{
  // ... existing fields ...
  
  @Nullable 
  private final BoundedStreamConfig boundedStreamConfig;
  
  public SeekableStreamSupervisorIOConfig(
      // ... existing parameters ...
      @JsonProperty("boundedStreamConfig") @Nullable BoundedStreamConfig boundedStreamConfig
  )
  {
    // ... existing initialization ...
    
    this.boundedStreamConfig = boundedStreamConfig;
    
    // Override taskCount if bounded config specifies it
    if (boundedStreamConfig != null && boundedStreamConfig.getTaskCount() != null) {
      this.taskCount = boundedStreamConfig.getTaskCount();
    }
    
    // Validation
    if (boundedStreamConfig != null) {
      // useEarliestSequenceNumber is ignored in bounded mode
      if (useEarliestSequenceNumber) {
        log.warn(
            "useEarliestSequenceNumber=true is ignored in bounded mode. " +
            "Using explicit startSequenceNumbers from boundedStreamConfig instead."
        );
      }
    }
  }
  
  @Nullable
  @JsonProperty
  public BoundedStreamConfig getBoundedStreamConfig()
  {
    return boundedStreamConfig;
  }
  
  public boolean isBounded()
  {
    return boundedStreamConfig != null;
  }
}
```

### Validation in `SeekableStreamSupervisorSpec`

```java
public abstract class SeekableStreamSupervisorSpec implements SupervisorSpec
{
  @Override
  public void validate()
  {
    // ... existing validation ...
    
    SeekableStreamSupervisorIOConfig ioConfig = getIoConfig();
    
    if (ioConfig.isBounded()) {
      // Require useConcurrentLocks for bounded mode
      if (context == null || !Boolean.TRUE.equals(context.get("useConcurrentLocks"))) {
        throw new IAE(
            "Bounded stream processing requires 'useConcurrentLocks' to be set to true " +
            "in the supervisor context to allow safe concurrent processing"
        );
      }
      
      // Validate partition consistency
      BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
      Set<?> startPartitions = boundedConfig.getStartSequenceNumbers().keySet();
      Set<?> endPartitions = boundedConfig.getEndSequenceNumbers().keySet();
      
      if (!startPartitions.equals(endPartitions)) {
        throw new IAE(
            "Bounded stream config has mismatched partitions. Start: %s, End: %s",
            startPartitions,
            endPartitions
        );
      }
    }
  }
}
```

---

## 2. Supervisor Behavior Changes

### Overview: Preventing Task Recreation

**The Critical Problem**:

In normal streaming mode, the supervisor's run loop:
1. Calls `updatePartitionDataFromStream()` to discover all partitions in the stream
2. Populates `partitionGroups` with discovered partitions
3. Calls `createNewTasks()` which loops through `partitionGroups`
4. For each partition group without an active task group, creates new tasks

In bounded mode, tasks complete at different times as they reach their end offsets. On the next supervisor run:
- `partitionGroups` still contains those partitions (even if we filtered to only configured partitions)
- The supervisor sees no active task group for those partitions
- **It tries to recreate tasks infinitely**

**The Solution**:

Two-part fix:
1. **Initialize once**: Populate `partitionGroups` from bounded config during startup (not from stream discovery)
2. **Skip recreation**: In `createNewTasks()`, skip creating task groups if bounded offsets already consumed

This ensures:
- `partitionGroups` is only populated once from config
- Partition discovery is skipped (or only used for validation)
- Task groups are created exactly once
- As tasks complete, no recreation attempts are made

---

### A. Initialize Partition Groups from Config

Initialize `partitionGroups` once from the bounded config during supervisor startup, then skip partition discovery entirely (or only use it for validation).

```java
// Add to SeekableStreamSupervisor class

/**
 * Initialize partitionGroups from bounded config instead of from stream discovery.
 * This prevents the supervisor from trying to recreate tasks as they complete.
 */
private void initializeBoundedPartitionGroups()
{
  if (!ioConfig.isBounded()) {
    return;
  }
  
  BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
  Map<?, ?> configuredPartitions = boundedConfig.getStartSequenceNumbers();
  
  for (Object partition : configuredPartitions.keySet()) {
    PartitionIdType typedPartition = (PartitionIdType) partition;
    int taskGroupId = getTaskGroupIdForPartition(typedPartition);
    
    partitionGroups.computeIfAbsent(taskGroupId, k -> new HashSet<>()).add(typedPartition);
    partitionIds.add(typedPartition);
    partitionOffsets.put(typedPartition, getNotSetMarker());
    
    log.info("Bounded mode: initialized partition[%s] in taskGroup[%d]", typedPartition, taskGroupId);
  }
  
  assignRecordSupplierToPartitionIds();
  
  log.info("Bounded mode: initialized [%d] partitions in [%d] task groups",
           configuredPartitions.size(),
           partitionGroups.size());
}

// Call during supervisor start
@Override
protected void start()
{
  synchronized (stateChangeLock) {
    Preconditions.checkState(!started, "already started");
    Preconditions.checkState(!exec.isShutdown(), "already stopped");
    
    // ... existing initialization ...
    
    // Initialize bounded partitions BEFORE first run
    if (ioConfig.isBounded()) {
      try {
        initializeBoundedPartitionGroups();
      }
      catch (Exception e) {
        log.error(e, "Failed to initialize bounded partition groups");
        throw new RuntimeException(e);
      }
    }
    
    started = true;
  }
}
```

### B. Skip Partition Discovery in Bounded Mode

```java
private boolean updatePartitionDataFromStream()
{
  // In bounded mode, partitionGroups is already initialized from config
  // Skip partition discovery to prevent recreating tasks
  if (ioConfig.isBounded()) {
    log.debug("Bounded mode: skipping partition discovery from stream");
    // Optionally, validate that partitions exist (see validation variant below)
    return true;
  }
  
  // ... existing logic for normal streaming mode ...
  
  List<PartitionIdType> previousPartitionIds = new ArrayList<>(partitionIds);
  Set<PartitionIdType> partitionIdsFromSupplier;
  
  recordSupplierLock.lock();
  try {
    partitionIdsFromSupplier = recordSupplier.getPartitionIds(ioConfig.getStream());
  }
  catch (Exception e) {
    stateManager.recordThrowableEvent(e);
    log.warn("Could not fetch partitions for topic/stream [%s]: %s", ioConfig.getStream(), e.getMessage());
    log.debug(e, "full stack trace");
    return false;
  }
  finally {
    recordSupplierLock.unlock();
  }
  
  if (partitionIdsFromSupplier == null || partitionIdsFromSupplier.size() == 0) {
    String errMsg = StringUtils.format("No partitions found for stream [%s]", ioConfig.getStream());
    stateManager.recordThrowableEvent(new StreamException(new ISE(errMsg)));
    log.warn(errMsg);
    return false;
  }
  
  log.debug("Found [%d] partitions for stream[%s]", partitionIdsFromSupplier.size(), ioConfig.getStream());
  
  // ... rest of existing logic for normal streaming mode ...
}
```

**Optional: Validation Variant**

If you want to validate that configured partitions exist in the stream (run once on first iteration):

```java
private volatile boolean boundedPartitionsValidated = false;

private boolean updatePartitionDataFromStream()
{
  // Query stream partitions
  Set<PartitionIdType> partitionIdsFromSupplier;
  recordSupplierLock.lock();
  try {
    partitionIdsFromSupplier = recordSupplier.getPartitionIds(ioConfig.getStream());
  }
  catch (Exception e) {
    stateManager.recordThrowableEvent(e);
    log.warn("Could not fetch partitions for topic/stream [%s]: %s", ioConfig.getStream(), e.getMessage());
    return false;
  }
  finally {
    recordSupplierLock.unlock();
  }
  
  if (partitionIdsFromSupplier == null || partitionIdsFromSupplier.size() == 0) {
    String errMsg = StringUtils.format("No partitions found for stream [%s]", ioConfig.getStream());
    stateManager.recordThrowableEvent(new StreamException(new ISE(errMsg)));
    log.warn(errMsg);
    return false;
  }
  
  // In bounded mode, validate once but don't update partitionGroups
  if (ioConfig.isBounded()) {
    if (!boundedPartitionsValidated) {
      validateBoundedPartitionsExist(partitionIdsFromSupplier);
      boundedPartitionsValidated = true;
    }
    return true; // Don't execute normal partition discovery
  }
  
  // ... existing logic for normal streaming mode ...
}

private void validateBoundedPartitionsExist(Set<PartitionIdType> availablePartitions)
{
  BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
  Set<?> configuredPartitions = boundedConfig.getStartSequenceNumbers().keySet();
  
  Set<?> missingPartitions = Sets.difference(configuredPartitions, availablePartitions);
  if (!missingPartitions.isEmpty()) {
    log.warn(
        "Bounded mode: configured partitions [%s] do not exist in stream [%s]. " +
        "Tasks for these partitions will fail to read data.",
        missingPartitions,
        ioConfig.getStream()
    );
  }
}
```

### C. Distinguish Completion from Failure

**Critical**: We must distinguish between tasks that completed successfully (reached end offsets) vs tasks that failed midway. Failed tasks need to be recreated to avoid data loss.

**Add abstract method for offset comparison:**

```java
// In SeekableStreamSupervisor

/**
 * Compares if current offset has reached or exceeded the target offset.
 * Used to determine if a bounded task group has completed successfully.
 * 
 * @param current Current offset from metadata storage
 * @param target Target end offset from bounded config
 * @return true if current >= target
 */
protected abstract boolean isOffsetAtOrBeyond(
    SequenceOffsetType current, 
    SequenceOffsetType target
);

// Example implementations:

// KafkaSupervisor
@Override
protected boolean isOffsetAtOrBeyond(Long current, Long target) {
  return current >= target;
}

// KinesisSupervisor  
@Override
protected boolean isOffsetAtOrBeyond(String current, String target) {
  // Kinesis sequence numbers are strings but lexicographically comparable
  return current.compareTo(target) >= 0;
}
```

**Check completion before skipping recreation:**

```java
// In createNewTasks()

private void createNewTasks() throws JsonProcessingException
{
  // ... existing validation ...
  
  // check that there is a current task group for each group of partitions in [partitionGroups]
  for (Integer groupId : partitionGroups.keySet()) {
    if (!activelyReadingTaskGroups.containsKey(groupId)) {
      
      // In bounded mode, distinguish between completion and failure
      if (ioConfig.isBounded() && boundedOffsetsConsumed) {
        if (hasTaskGroupReachedBoundedEnd(groupId)) {
          // Task group completed successfully - don't recreate
          log.debug(
              "Bounded taskGroup[%d] has reached end offsets, skipping recreation",
              groupId
          );
          continue; // Skip creating new task group
        } else {
          // Task group hasn't reached end - task must have failed, recreate it
          log.info(
              "Bounded taskGroup[%d] has not reached end offsets (current: %s, target: %s). " +
              "Task may have failed, recreating to continue processing.",
              groupId,
              getCurrentOffsetsForGroup(groupId),
              getEndOffsetsForGroup(groupId)
          );
          // Fall through to create new task group
        }
      }
      
      log.info("Creating new taskGroup[%d] for partitions[%s].", groupId, partitionGroups.get(groupId));
      
      // ... existing task group creation logic ...
    }
  }
  
  // Mark that bounded offsets have been consumed after first task group creation
  if (ioConfig.isBounded() && !activelyReadingTaskGroups.isEmpty() && !boundedOffsetsConsumed) {
    log.info("Bounded mode: marking offsets as consumed, will check completion before recreating");
    boundedOffsetsConsumed = true;
  }
  
  // ... rest of existing logic ...
}

/**
 * Check if all partitions in a task group have reached their bounded end offsets.
 * Used to determine if the task group completed successfully vs failed midway.
 */
private boolean hasTaskGroupReachedBoundedEnd(int groupId)
{
  BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
  Map<?, ?> endOffsets = boundedConfig.getEndSequenceNumbers();
  Map<PartitionIdType, SequenceOffsetType> currentOffsets = getOffsetsFromMetadataStorage();
  
  if (currentOffsets == null || currentOffsets.isEmpty()) {
    log.debug("No checkpointed offsets found, taskGroup[%d] has not completed", groupId);
    return false; // No progress yet, task hasn't completed
  }
  
  Set<PartitionIdType> partitionsInGroup = partitionGroups.get(groupId);
  if (partitionsInGroup == null || partitionsInGroup.isEmpty()) {
    return false;
  }
  
  // Check if ALL partitions in this group have reached their end offsets
  for (PartitionIdType partition : partitionsInGroup) {
    SequenceOffsetType endOffset = (SequenceOffsetType) endOffsets.get(partition);
    SequenceOffsetType currentOffset = currentOffsets.get(partition);
    
    if (currentOffset == null) {
      log.debug(
          "Partition[%s] in taskGroup[%d] has no checkpointed offset, not complete",
          partition,
          groupId
      );
      return false; // Partition hasn't started processing
    }
    
    if (!isOffsetAtOrBeyond(currentOffset, endOffset)) {
      log.debug(
          "Partition[%s] in taskGroup[%d] at offset[%s], has not reached end[%s]",
          partition,
          groupId,
          currentOffset,
          endOffset
      );
      return false; // This partition hasn't reached its end
    }
  }
  
  log.info(
      "All partitions in taskGroup[%d] have reached their end offsets",
      groupId
  );
  return true; // All partitions have reached their end offsets
}

private Map<PartitionIdType, SequenceOffsetType> getCurrentOffsetsForGroup(int groupId)
{
  Map<PartitionIdType, SequenceOffsetType> allOffsets = getOffsetsFromMetadataStorage();
  if (allOffsets == null || allOffsets.isEmpty()) {
    return Collections.emptyMap();
  }
  
  Set<PartitionIdType> partitionsInGroup = partitionGroups.get(groupId);
  if (partitionsInGroup == null) {
    return Collections.emptyMap();
  }
  
  return partitionsInGroup.stream()
      .filter(allOffsets::containsKey)
      .collect(Collectors.toMap(
          Function.identity(),
          allOffsets::get
      ));
}

private Map<PartitionIdType, SequenceOffsetType> getEndOffsetsForGroup(int groupId)
{
  BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
  Map<?, ?> endOffsets = boundedConfig.getEndSequenceNumbers();
  Set<PartitionIdType> partitionsInGroup = partitionGroups.get(groupId);
  
  if (partitionsInGroup == null) {
    return Collections.emptyMap();
  }
  
  return partitionsInGroup.stream()
      .filter(p -> endOffsets.containsKey(p))
      .collect(Collectors.toMap(
          Function.identity(),
          p -> (SequenceOffsetType) endOffsets.get(p)
      ));
}
```

### D. Bounded Offset Generation

```java
// Add to SeekableStreamSupervisor class

private volatile boolean boundedOffsetsConsumed = false;

private Map<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> generateStartingSequencesForPartitionGroup(
    int groupId
)
{
  // NEW: Check for bounded mode (only use bounded offsets for first task group)
  if (ioConfig.isBounded() && !boundedOffsetsConsumed) {
    return generateBoundedStartingOffsets(groupId);
  }
  
  // Existing logic for normal streaming mode (also used for bounded retries after failure)
  ImmutableMap.Builder<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> builder = ImmutableMap.builder();
  final Map<PartitionIdType, SequenceOffsetType> metadataOffsets = getOffsetsFromMetadataStorage();
  
  for (PartitionIdType partitionId : partitionGroups.get(groupId)) {
    SequenceOffsetType sequence = partitionOffsets.get(partitionId);
    
    if (!getNotSetMarker().equals(sequence)) {
      if (!isEndOfShard(sequence)) {
        builder.put(partitionId, makeSequenceNumber(sequence, useExclusiveStartSequenceNumberForNonFirstSequence()));
      }
    } else {
      OrderedSequenceNumber<SequenceOffsetType> offsetFromStorage = getOffsetFromStorageForPartition(
          partitionId,
          metadataOffsets
      );
      
      if (offsetFromStorage != null) {
        builder.put(partitionId, offsetFromStorage);
      } else if (ioConfig.isBounded()) {
        // NEW: In bounded mode, if no checkpoint exists (task failed before first checkpoint),
        // fall back to bounded start offset
        BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
        Object startOffset = boundedConfig.getStartSequenceNumbers().get(partitionId);
        if (startOffset != null) {
          log.info(
              "Bounded mode: no checkpoint found for partition[%s], using configured start offset[%s]",
              partitionId,
              startOffset
          );
          builder.put(partitionId, makeSequenceNumber((SequenceOffsetType) startOffset, false));
        }
      }
    }
  }
  
  return builder.build();
}

/**
 * Generate starting offsets from bounded config.
 * Uses inclusive start offsets (we want to process FROM this offset).
 */
private Map<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> generateBoundedStartingOffsets(
    int groupId
)
{
  BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
  Map<?, ?> configuredStartOffsets = boundedConfig.getStartSequenceNumbers();
  
  ImmutableMap.Builder<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> builder = ImmutableMap.builder();
  
  Set<PartitionIdType> partitionsInGroup = partitionGroups.get(groupId);
  if (partitionsInGroup == null) {
    log.warn("No partitions found for taskGroup[%d] in bounded mode", groupId);
    return builder.build();
  }
  
  for (PartitionIdType partition : partitionsInGroup) {
    Object startOffset = configuredStartOffsets.get(partition);
    if (startOffset != null) {
      SequenceOffsetType typedOffset = (SequenceOffsetType) startOffset;
      // Use inclusive start for bounded mode (false = inclusive)
      builder.put(partition, makeSequenceNumber(typedOffset, false));
      log.info("Bounded mode: partition[%s] starting at offset[%s]", partition, typedOffset);
    } else {
      log.warn("Bounded mode: no start offset configured for partition[%s], skipping", partition);
    }
  }
  
  return builder.build();
}
```

### E. Pass End Offsets to Tasks

Modify task creation to include end offsets from bounded config:

```java
// In createNewTasks() method, when creating TaskGroup

private void createNewTasks() throws JsonProcessingException
{
  // ... existing validation and offset generation ...
  
  for (Integer groupId : partitionGroups.keySet()) {
    if (!activelyReadingTaskGroups.containsKey(groupId)) {
      log.info("Creating new taskGroup[%d] for partitions[%s].", groupId, partitionGroups.get(groupId));
      
      // ... existing minimumMessageTime, maximumMessageTime setup ...
      
      final Map<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> unfilteredStartingOffsets =
          generateStartingSequencesForPartitionGroup(groupId);
      
      // ... existing filtering logic ...
      
      // NEW: Extract end offsets for bounded mode
      Map<PartitionIdType, SequenceOffsetType> endOffsets = null;
      if (ioConfig.isBounded()) {
        endOffsets = extractBoundedEndOffsets(groupId);
      }
      
      log.info(
          "Initializing taskGroup[%d] with startingOffsets[%s] and endOffsets[%s]",
          groupId,
          simpleStartingOffsets,
          endOffsets
      );
      
      activelyReadingTaskGroups.put(
          groupId,
          new TaskGroup(
              groupId,
              simpleStartingOffsets,
              simpleUnfilteredStartingOffsets,
              endOffsets,  // NEW PARAMETER
              minimumMessageTime,
              maximumMessageTime,
              exclusiveStartSequenceNumberPartitions
          )
      );
    }
  }
  
  // ... rest of existing task creation logic ...
}

/**
 * Extract end offsets from bounded config for the given task group.
 */
private Map<PartitionIdType, SequenceOffsetType> extractBoundedEndOffsets(int groupId)
{
  BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
  Map<?, ?> configuredEndOffsets = boundedConfig.getEndSequenceNumbers();
  
  Set<PartitionIdType> partitionsInGroup = partitionGroups.get(groupId);
  if (partitionsInGroup == null) {
    return Collections.emptyMap();
  }
  
  ImmutableMap.Builder<PartitionIdType, SequenceOffsetType> builder = ImmutableMap.builder();
  for (PartitionIdType partition : partitionsInGroup) {
    Object endOffset = configuredEndOffsets.get(partition);
    if (endOffset != null) {
      builder.put(partition, (SequenceOffsetType) endOffset);
      log.info("Bounded mode: partition[%s] will stop at offset[%s]", partition, endOffset);
    }
  }
  
  return builder.build();
}
```

### F. TaskGroup Modification

```java
// Modify TaskGroup class to store end offsets

protected static class TaskGroup
{
  final int groupId;
  final Map<PartitionIdType, SequenceOffsetType> startingSequences;
  final Map<PartitionIdType, SequenceOffsetType> unfilteredStartingSequences;
  final Map<PartitionIdType, SequenceOffsetType> endSequences;  // NEW
  final ConcurrentHashMap<String, TaskData> tasks = new ConcurrentHashMap<>();
  // ... other fields ...
  
  TaskGroup(
      int groupId,
      ImmutableMap<PartitionIdType, SequenceOffsetType> startingSequences,
      ImmutableMap<PartitionIdType, SequenceOffsetType> unfilteredStartingSequences,
      @Nullable Map<PartitionIdType, SequenceOffsetType> endSequences,  // NEW
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      Set<PartitionIdType> exclusiveStartSequenceNumberPartitions
  )
  {
    this.groupId = groupId;
    this.startingSequences = startingSequences;
    this.unfilteredStartingSequences = unfilteredStartingSequences;
    this.endSequences = endSequences != null ? ImmutableMap.copyOf(endSequences) : null;  // NEW
    this.minimumMessageTime = minimumMessageTime;
    this.maximumMessageTime = maximumMessageTime;
    this.exclusiveStartSequenceNumberPartitions = exclusiveStartSequenceNumberPartitions;
  }
}
```

### G. Task IOConfig Creation

Modify task IOConfig creation to use bounded end offsets:

```java
// In createTasksForGroup() or wherever task IOConfig is created

protected abstract List<SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType>> createIndexTasks(
    int replicas,
    String baseSequenceName,
    ObjectMapper sortingMapper,
    TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> sequenceOffsets,
    SeekableStreamIndexTaskIOConfig taskIoConfig,
    SeekableStreamIndexTaskTuningConfig taskTuningConfig,
    RowIngestionMetersFactory rowIngestionMetersFactory,
    @Nullable List<Integer> serverPrioritiesToAssign
) throws JsonProcessingException;

// When creating the IOConfig, use TaskGroup's endSequences:

private void createTasksForGroup(int groupId, int numTasksToCreate)
{
  TaskGroup taskGroup = activelyReadingTaskGroups.get(groupId);
  
  // Create start sequence numbers
  SeekableStreamStartSequenceNumbers<PartitionIdType, SequenceOffsetType> startSequenceNumbers =
      new SeekableStreamStartSequenceNumbers<>(
          ioConfig.getStream(),
          taskGroup.startingSequences,
          taskGroup.exclusiveStartSequenceNumberPartitions
      );
  
  // Create end sequence numbers
  SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType> endSequenceNumbers;
  
  if (taskGroup.endSequences != null && !taskGroup.endSequences.isEmpty()) {
    // Bounded mode: use explicit end offsets
    endSequenceNumbers = new SeekableStreamEndSequenceNumbers<>(
        ioConfig.getStream(),
        taskGroup.endSequences
    );
    log.info("Creating bounded tasks with endOffsets: %s", taskGroup.endSequences);
  } else {
    // Streaming mode: use exclusive end (effectively no end)
    Map<PartitionIdType, SequenceOffsetType> exclusiveEndOffsets = taskGroup.startingSequences
        .keySet()
        .stream()
        .collect(Collectors.toMap(
            Function.identity(),
            partition -> getEndOfPartitionMarker()  // MAX_VALUE or equivalent
        ));
    
    endSequenceNumbers = new SeekableStreamEndSequenceNumbers<>(
        ioConfig.getStream(),
        exclusiveEndOffsets
    );
  }
  
  // Create task IOConfig with start and end offsets
  SeekableStreamIndexTaskIOConfig taskIoConfig = createTaskIoConfig(
      groupId,
      startSequenceNumbers,
      endSequenceNumbers,
      taskGroup.minimumMessageTime,
      taskGroup.maximumMessageTime
  );
  
  // ... create and submit tasks ...
}
```

### H. Completion Detection and Auto-Termination

```java
// Add to SeekableStreamSupervisor class

private volatile boolean boundedTasksCompleted = false;

/**
 * Check if all bounded tasks have completed.
 * Called after checkPendingCompletionTasks() in runInternal.
 */
private void checkForBoundedCompletion()
{
  if (!ioConfig.isBounded() || !boundedOffsetsConsumed) {
    return;
  }
  
  // Check if all task groups are gone (tasks completed)
  boolean allTasksComplete = activelyReadingTaskGroups.isEmpty() && 
      pendingCompletionTaskGroups.values().stream().allMatch(List::isEmpty);
  
  if (allTasksComplete && !boundedTasksCompleted) {
    log.info("All bounded tasks completed for supervisor[%s]", supervisorId);
    boundedTasksCompleted = true;
  }
}

public void runInternal()
{
  try {
    possiblyRegisterListener();
    
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.CONNECTING_TO_STREAM);
    
    // In bounded mode, this either skips discovery or validates partitions
    if (!updatePartitionDataFromStream() && !stateManager.isAtLeastOneSuccessfulRun()) {
      return;
    }
    
    // NEW: Check for bounded completion FIRST (before discovering tasks)
    if (boundedTasksCompleted) {
      handleBoundedCompletion();
      return;
    }
    
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    discoverTasks();
    
    updateTaskStatus();
    checkTaskDuration();
    checkPendingCompletionTasks();
    
    // NEW: Check for bounded completion after tasks may have finished
    checkForBoundedCompletion();
    
    if (boundedTasksCompleted) {
      handleBoundedCompletion();
      return;
    }
    
    checkCurrentTaskState();
    checkIfStreamInactiveAndTurnSupervisorIdle();
    
    if (isStopping()) {
      logDebugReport();
      return;
    }
    
    synchronized (stateChangeLock) {
      if (isStopping()) {
        log.debug("Supervisor[%s] for datasource[%s] is already stopping.", supervisorId, dataSource);
      } else if (stateManager.isIdle()) {
        log.debug("Supervisor[%s] for datasource[%s] is idle.", supervisorId, dataSource);
      } else if (!spec.isSuspended()) {
        log.debug("Supervisor[%s] for datasource[%s] is running.", supervisorId, dataSource);
        stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.CREATING_TASKS);
        createNewTasks();
      } else {
        log.debug("Supervisor[%s] for datasource[%s] is suspended.", supervisorId, dataSource);
        gracefulShutdownInternal();
      }
    }
    
    logDebugReport();
  }
  catch (Exception e) {
    // ... existing exception handling ...
  }
  finally {
    stateManager.markRunFinished();
  }
}

private void handleBoundedCompletion()
{
  log.info("Bounded processing complete for supervisor[%s]", supervisorId);
  
  BoundedStreamConfig boundedConfig = ioConfig.getBoundedStreamConfig();
  
  if (boundedConfig.isTerminateOnCompletion()) {
    log.info("Auto-terminating bounded supervisor[%s] (terminateOnCompletion=true)", supervisorId);
    stateManager.maybeSetState(SupervisorStateManager.BasicState.STOPPING);
    gracefulShutdownInternal();
    addNotice(new BoundedProcessingCompleteNotice());
  } else {
    log.info(
        "Bounded supervisor[%s] complete but terminateOnCompletion=false, transitioning to IDLE",
        supervisorId
    );
    stateManager.maybeSetState(SupervisorStateManager.BasicState.IDLE);
    addNotice(new BoundedProcessingCompleteNotice());
  }
}
```

### I. New Notice Class

```java
package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Notice emitted when a bounded supervisor completes processing all configured partition ranges.
 */
public class BoundedProcessingCompleteNotice extends Notice
{
  @JsonCreator
  public BoundedProcessingCompleteNotice()
  {
    super();
  }
  
  @Override
  public String toString()
  {
    return "BoundedProcessingCompleteNotice{}";
  }
}
```

---

## 3. Kafka/Kinesis Implementation

### Kafka Supervisor

```java
// In KafkaSupervisor class

// The createIndexTasks method already receives the taskIoConfig with endSequenceNumbers,
// so no changes needed - just ensure the IOConfig is properly passed to KafkaIndexTask

@Override
protected List<SeekableStreamIndexTask<KafkaTopicPartition, Long, KafkaRecordEntity>> createIndexTasks(
    int replicas,
    String baseSequenceName,
    ObjectMapper sortingMapper,
    TreeMap<Integer, Map<KafkaTopicPartition, Long>> sequenceOffsets,
    SeekableStreamIndexTaskIOConfig taskIoConfig,
    SeekableStreamIndexTaskTuningConfig taskTuningConfig,
    RowIngestionMetersFactory rowIngestionMetersFactory,
    @Nullable List<Integer> serverPrioritiesToAssign
) throws JsonProcessingException
{
  final String checkpoints = sortingMapper.writerFor(CHECKPOINTS_TYPE_REF).writeValueAsString(sequenceOffsets);
  final Map<String, Object> context = createBaseTaskContexts();
  context.put(CHECKPOINTS_CTX_KEY, checkpoints);
  
  List<SeekableStreamIndexTask<KafkaTopicPartition, Long, KafkaRecordEntity>> taskList = new ArrayList<>();
  
  for (int i = 0; i < replicas; i++) {
    String taskId = IdUtils.getRandomIdWithPrefix(baseSequenceName);
    
    // The taskIoConfig already has the correct endSequenceNumbers (bounded or streaming)
    taskList.add(new KafkaIndexTask(
        taskId,
        spec.getId(),
        new TaskResource(baseSequenceName, 1),
        spec.getDataSchema(),
        (KafkaIndexTaskTuningConfig) taskTuningConfig,
        (KafkaIndexTaskIOConfig) taskIoConfig,  // Already has correct start/end offsets
        context,
        sortingMapper,
        CollectionUtils.isNullOrEmpty(serverPrioritiesToAssign) ? null : serverPrioritiesToAssign.get(i)
    ));
  }
  
  return taskList;
}
```

### Kinesis Supervisor

Similar implementation - the `taskIoConfig` already contains the proper `endSequenceNumbers`.

---

## 4. resetOffsetsAndBackfill Implementation

Now the `resetOffsetsAndBackfill` operation becomes much simpler - it just creates a bounded supervisor:

```java
// In SupervisorManager.java

public Map<String, Object> resetSupervisorAndBackfill(String id, @Nullable Integer backfillTaskCount)
{
  Preconditions.checkState(started, "SupervisorManager not started");
  Preconditions.checkNotNull(id, "id");
  
  Pair<Supervisor, SupervisorSpec> supervisorPair = supervisors.get(id);
  if (supervisorPair == null || supervisorPair.lhs == null || supervisorPair.rhs == null) {
    throw new IllegalStateException(StringUtils.format("Supervisor[%s] does not exist", id));
  }
  
  if (!(supervisorPair.lhs instanceof SeekableStreamSupervisor)) {
    throw new IllegalArgumentException(StringUtils.format("Supervisor[%s] is not a SeekableStreamSupervisor", id));
  }
  
  SeekableStreamSupervisor streamSupervisor = (SeekableStreamSupervisor) supervisorPair.lhs;
  SeekableStreamSupervisorSpec streamSpec = (SeekableStreamSupervisorSpec) supervisorPair.rhs;
  
  // Verify useEarliestOffset is false
  if (streamSupervisor.getIoConfig().isUseEarliestSequenceNumber()) {
    throw new IllegalArgumentException("Reset with backfill is not supported when useEarliestOffset is true.");
  }
  
  // Verify useConcurrentLocks is enabled
  if (streamSpec.getContext() == null || !Boolean.TRUE.equals(streamSpec.getContext().get("useConcurrentLocks"))) {
    throw new IllegalArgumentException(
        "Backfill requires 'useConcurrentLocks' to be set to true in the supervisor context " +
        "to allow concurrent writes with the main supervisor tasks"
    );
  }
  
  // Verify supervisor is running
  if (supervisorPair.lhs.getState() != SupervisorStateManager.BasicState.RUNNING) {
    throw new IllegalStateException("A running supervisor is required to query the latest offsets from the stream");
  }
  
  log.info("Capturing latest offsets from stream for supervisor[%s]", id);
  streamSupervisor.updatePartitionLagFromStream();
  Map<?, ?> latestOffsets = streamSupervisor.getLatestSequencesFromStream();
  
  log.info("Capturing checkpointed offsets for supervisor[%s]", id);
  Map<?, ?> startOffsets = streamSupervisor.getOffsetsFromMetadataStorage();
  
  // Validate that we successfully retrieved offsets
  if (latestOffsets == null || latestOffsets.isEmpty()) {
    throw new IllegalStateException(
        StringUtils.format("Failed to get latest offsets from stream for supervisor[%s]", id)
    );
  }
  if (startOffsets == null || startOffsets.isEmpty()) {
    throw new IllegalStateException(
        StringUtils.format("Failed to get checkpointed offsets for supervisor[%s]", id)
    );
  }
  
  log.info("Resetting supervisor[%s] metadata to latest offsets", id);
  DataSourceMetadata resetMetadata = streamSupervisor.createDataSourceMetaDataForReset(
      streamSupervisor.getIoConfig().getStream(),
      latestOffsets
  );
  
  streamSupervisor.resetOffsets(resetMetadata);
  
  // Reset autoscaler if present
  SupervisorTaskAutoScaler autoscaler = autoscalers.get(id);
  if (autoscaler != null) {
    autoscaler.reset();
  }
  
  // Create bounded backfill supervisor
  String backfillSupervisorId = id + "_backfill";
  SeekableStreamSupervisorSpec backfillSpec = createBoundedBackfillSpec(
      streamSpec,
      backfillSupervisorId,
      startOffsets,
      latestOffsets,
      backfillTaskCount
  );
  
  // Submit backfill supervisor
  createOrUpdateAndStartSupervisor(backfillSpec);
  
  Map<?, Object> backfillRange = calculateBackfillRange(startOffsets, latestOffsets);
  
  log.info(
      "Successfully reset supervisor[%s] to latest and created backfill supervisor[%s]. Backfill range: %s",
      id,
      backfillSupervisorId,
      backfillRange
  );
  
  return ImmutableMap.of(
      "id", id,
      "backfillSupervisorId", backfillSupervisorId,
      "backfillRange", backfillRange
  );
}

/**
 * Creates a bounded supervisor spec for backfill processing.
 */
private SeekableStreamSupervisorSpec createBoundedBackfillSpec(
    SeekableStreamSupervisorSpec originalSpec,
    String backfillSupervisorId,
    Map<?, ?> startOffsets,
    Map<?, ?> endOffsets,
    @Nullable Integer backfillTaskCount
)
{
  // Create bounded stream config
  int taskCount = backfillTaskCount != null 
      ? backfillTaskCount 
      : Math.max(1, originalSpec.getIoConfig().getTaskCount() / 2);
  
  BoundedStreamConfig boundedConfig = new BoundedStreamConfig(
      startOffsets,
      endOffsets,
      true,  // terminateOnCompletion
      taskCount
  );
  
  // Clone the original spec with bounded config
  // This is implementation-specific (Kafka/Kinesis), but the pattern is:
  // 1. Copy data schema
  // 2. Copy tuning config
  // 3. Create new IO config with boundedStreamConfig
  // 4. Use same context (with useConcurrentLocks)
  
  return originalSpec.createBoundedCopy(backfillSupervisorId, boundedConfig);
}

/**
 * Calculates the backfill range for display in API response.
 */
@VisibleForTesting
public Map<?, Object> calculateBackfillRange(Map<?, ?> startOffsets, Map<?, ?> endOffsets)
{
  Map<Object, Object> backfillRange = new HashMap<>();
  
  for (Map.Entry<?, ?> entry : endOffsets.entrySet()) {
    Object partition = entry.getKey();
    Object endOffset = entry.getValue();
    Object startOffset = startOffsets.get(partition);
    
    if (startOffset != null) {
      backfillRange.put(
          partition,
          ImmutableMap.of(
              "start", startOffset,
              "end", endOffset
          )
      );
    }
  }
  
  return backfillRange;
}
```

### Add Helper Method to SupervisorSpec

```java
// In SeekableStreamSupervisorSpec (abstract method)

/**
 * Creates a bounded copy of this supervisor spec with the given ID and bounded config.
 * Used for creating backfill supervisors.
 */
public abstract SeekableStreamSupervisorSpec createBoundedCopy(
    String newSupervisorId,
    BoundedStreamConfig boundedConfig
);

// Example implementation in KafkaSupervisorSpec:

@Override
public SeekableStreamSupervisorSpec createBoundedCopy(
    String newSupervisorId,
    BoundedStreamConfig boundedConfig
)
{
  // Create new IO config with bounded config
  KafkaSupervisorIOConfig newIoConfig = new KafkaSupervisorIOConfig(
      ioConfig.getTopic(),
      ioConfig.getInputFormat(),
      ioConfig.getReplicas(),
      ioConfig.getTaskCount(),
      ioConfig.getTaskDuration(),
      ioConfig.getStartDelay(),
      ioConfig.getPeriod(),
      ioConfig.isUseEarliestOffset(),
      ioConfig.getCompletionTimeout(),
      ioConfig.getLateMessageRejectionPeriod(),
      ioConfig.getEarlyMessageRejectionPeriod(),
      ioConfig.getLateMessageRejectionStartDateTime(),
      ioConfig.getConsumerProperties(),
      ioConfig.getPollTimeout(),
      ioConfig.getAutoScalerConfig(),
      ioConfig.getIdleConfig(),
      ioConfig.getStopTaskCount(),
      ioConfig.getServerPriorityToReplicas(),
      boundedConfig  // NEW
  );
  
  return new KafkaSupervisorSpec(
      newSupervisorId,  // Use backfill supervisor ID
      dataSchema,
      tuningConfig,
      newIoConfig,
      context,  // Same context (includes useConcurrentLocks)
      suspended,
      taskStorageProvider,
      indexerMetadataStorageProvider,
      taskClientFactory,
      objectMapper,
      monitorSchedulerConfig,
      rowIngestionMetersFactory,
      supervisorStateManagerConfig,
      authorizerMapper,
      chatHandlerProvider,
      lookupNodeDiscovery,
      serviceMetricEvent
  );
}
```

---

## 5. Example Configuration

### Bounded Supervisor (Direct Creation)

```json
{
  "type": "kafka",
  "spec": {
    "dataSchema": {
      "dataSource": "wikipedia_backfill",
      "timestampSpec": {
        "column": "timestamp",
        "format": "auto"
      },
      "dimensionsSpec": {
        "dimensions": ["page", "user", "country"]
      },
      "granularitySpec": {
        "segmentGranularity": "hour",
        "queryGranularity": "minute"
      }
    },
    "ioConfig": {
      "topic": "wikipedia",
      "inputFormat": {
        "type": "json"
      },
      "taskCount": 2,
      "replicas": 1,
      "taskDuration": "PT1H",
      "useEarliestOffset": false,
      "boundedStreamConfig": {
        "startSequenceNumbers": {
          "0": 1000,
          "1": 1500,
          "2": 2000
        },
        "endSequenceNumbers": {
          "0": 5000,
          "1": 5500,
          "2": 6000
        },
        "terminateOnCompletion": true,
        "taskCount": 3
      }
    },
    "tuningConfig": {
      "type": "kafka",
      "maxRowsInMemory": 100000
    }
  },
  "context": {
    "useConcurrentLocks": true
  }
}
```

### resetOffsetsAndBackfill API Call

```bash
# Default backfill task count (taskCount / 2)
curl -X POST "http://localhost:8081/druid/indexer/v1/supervisor/wikipedia/resetOffsetsAndBackfill"

# Custom backfill task count
curl -X POST "http://localhost:8081/druid/indexer/v1/supervisor/wikipedia/resetOffsetsAndBackfill?backfillTaskCount=4"
```

Response:
```json
{
  "id": "wikipedia",
  "backfillSupervisorId": "wikipedia_backfill",
  "backfillRange": {
    "0": {
      "start": 1000,
      "end": 5000
    },
    "1": {
      "start": 1500,
      "end": 5500
    },
    "2": {
      "start": 2000,
      "end": 6000
    }
  }
}
```

---

## 6. Task Failure Handling

### Scenario: Task Fails Midway Through Processing

**Problem Without Failure Handling:**
```
Initial task: start=1000, end=5000
Task processes data, checkpoints at offset=3000
Task fails (OOM, node crash, etc.)
Supervisor sees: no active task group, boundedOffsetsConsumed=true
Current logic: skips recreation
Result: Data loss! Offsets 3000-5000 never processed ❌
```

**Solution: Check Completion Before Skipping Recreation**

The `hasTaskGroupReachedBoundedEnd()` method checks if all partitions have reached their end offsets by comparing metadata storage offsets with configured end offsets.

### Complete Flow With Failure and Retry

```
T0: Initial task creation
  - boundedOffsetsConsumed = false
  - generateStartingSequencesForPartitionGroup() → bounded start (1000)
  - extractBoundedEndOffsets() → bounded end (5000)
  - Task created: start=1000, end=5000
  - boundedOffsetsConsumed = true

T1: Task processes data
  - Reads offsets 1000-3000
  - Checkpoints progress: metadata storage offset=3000
  - Task fails (OOM) ❌

T2: Supervisor run loop
  - updateTaskStatus() detects task failure
  - Task removed from activelyReadingTaskGroups
  
T3: createNewTasks()
  - No active task group for groupId
  - Checks: isBounded() && boundedOffsetsConsumed? YES
  - Calls hasTaskGroupReachedBoundedEnd(groupId):
    ├─ currentOffsets from metadata: {partition: 3000}
    ├─ endOffsets from config: {partition: 5000}
    ├─ isOffsetAtOrBeyond(3000, 5000)? NO
    └─ Returns false (not complete)
  - Logs: "Task has not reached end offsets, recreating"
  - Falls through to create new task group ✅
  
T4: New task creation (retry)
  - generateStartingSequencesForPartitionGroup(groupId)
    ├─ boundedOffsetsConsumed=true (uses normal offset generation)
    ├─ Reads from metadata storage: offset=3000
    └─ Returns start=3000 (continues from checkpoint) ✅
  - extractBoundedEndOffsets() → end=5000
  - Task created: start=3000, end=5000
  - Task processes remaining data (3000-5000)
  - Task completes successfully ✅

T5: Task completion
  - Task reaches offset=5000, stops reading
  - Publishes segments
  - Updates metadata storage: offset=5000
  - Task removed from activelyReadingTaskGroups
  
T6: createNewTasks() (after completion)
  - No active task group for groupId
  - Checks: isBounded() && boundedOffsetsConsumed? YES
  - Calls hasTaskGroupReachedBoundedEnd(groupId):
    ├─ currentOffsets from metadata: {partition: 5000}
    ├─ endOffsets from config: {partition: 5000}
    ├─ isOffsetAtOrBeyond(5000, 5000)? YES ✅
    └─ Returns true (complete!)
  - Logs: "TaskGroup has reached end offsets, skipping recreation"
  - Skips recreation ✅
```

### Edge Cases Handled

1. **Task fails before first checkpoint**
   - `getOffsetsFromMetadataStorage()` returns null/empty
   - `generateStartingSequencesForPartitionGroup()` falls back to bounded start offset
   - Task starts from beginning of bounded range

2. **Multiple failures/retries**
   - Each retry calls `hasTaskGroupReachedBoundedEnd()`
   - Tasks continue from last checkpoint
   - No duplicate processing

3. **Partial progress across partitions**
   - `hasTaskGroupReachedBoundedEnd()` checks ALL partitions
   - If any partition incomplete, returns false
   - Task group recreated to process remaining partitions

4. **Supervisor restart during task execution**
   - `discoverTasks()` finds running tasks, adopts them
   - `boundedOffsetsConsumed` reconstructed from task state
   - Normal supervision continues

5. **Supervisor restart after task failure**
   - No active tasks found
   - `boundedOffsetsConsumed` restored (may need persistence)
   - `hasTaskGroupReachedBoundedEnd()` queries metadata storage
   - Task recreated if incomplete

### Benefits

✅ **Automatic Retry** - Failed tasks automatically recreated  
✅ **Checkpoint-based Recovery** - Tasks resume from last checkpoint, not from beginning  
✅ **No Data Loss** - All offsets in bounded range are processed  
✅ **No Duplicate Processing** - Tasks continue from checkpoint  
✅ **Prevents Infinite Recreation** - Successfully completed tasks not recreated  
✅ **Supervisor Restart Safe** - Metadata storage persists checkpoint state

---

## 7. Monitoring and Status

### Supervisor Status Enhancement

Add bounded progress to supervisor status:

```json
{
  "dataSource": "wikipedia_backfill",
  "stream": "wikipedia",
  "state": "RUNNING",
  "detailedState": "RUNNING",
  "healthy": true,
  "bounded": true,
  "boundedProgress": {
    "partitions": {
      "0": {
        "startOffset": 1000,
        "endOffset": 5000,
        "currentOffset": 3000,
        "progress": 0.50
      },
      "1": {
        "startOffset": 1500,
        "endOffset": 5500,
        "currentOffset": 4500,
        "progress": 0.75
      }
    },
    "overallProgress": 0.625
  }
}
```

---

## 8. Benefits Summary

### ✅ **Simplicity**
- Tasks auto-terminate when reaching end offsets (already implemented)
- Supervisor just configures tasks properly and prevents recreation
- No complex offset tracking or polling needed

### ✅ **Backward Compatibility**
- Existing supervisors work unchanged (`boundedStreamConfig` is null)
- No behavioral changes to normal streaming mode
- Existing APIs and endpoints unaffected

### ✅ **Prevents Task Recreation**
- Override partition discovery to only track configured partitions
- Track completion to prevent infinite task recreation
- Clear state transitions

### ✅ **Flexible**
- Can disable auto-termination if needed
- Can override taskCount for bounded mode
- Supports backfill and manual bounded processing

### ✅ **Observable**
- Bounded progress visible in supervisor status
- Completion notice in supervisor history
- Clear logging at each step

### ✅ **Safe**
- Requires `useConcurrentLocks` for bounded mode
- Validates offset ranges match partition sets
- Validates configured partitions exist in stream
- Bounded offsets only consumed once (first task group)

### ✅ **Simpler resetOffsetsAndBackfill**
- No special `submitBackfillTask()` logic needed
- Just creates a bounded supervisor
- Leverages all existing supervisor machinery
- Automatic retry and offset tracking through normal supervisor behavior

---

## 9. Implementation Checklist

### Phase 1: Core Infrastructure
- [ ] Create `BoundedStreamConfig` class
- [ ] Add `boundedStreamConfig` field to `SeekableStreamSupervisorIOConfig`
- [ ] Add validation in `SeekableStreamSupervisorSpec`
- [ ] Add `BoundedProcessingCompleteNotice` class

### Phase 2: Partition Initialization and Discovery Override
- [ ] Add `initializeBoundedPartitionGroups()` method
- [ ] Call `initializeBoundedPartitionGroups()` in `start()` method
- [ ] Modify `updatePartitionDataFromStream()` to skip discovery in bounded mode
- [ ] Optionally add `validateBoundedPartitionsExist()` for validation
- [ ] Test partition initialization and validation logic

### Phase 3: Failure Handling
- [ ] Add abstract method `isOffsetAtOrBeyond()` to `SeekableStreamSupervisor`
- [ ] Implement `isOffsetAtOrBeyond()` in `KafkaSupervisor`
- [ ] Implement `isOffsetAtOrBeyond()` in `KinesisSupervisor`
- [ ] Add `hasTaskGroupReachedBoundedEnd()` method
- [ ] Add `getCurrentOffsetsForGroup()` helper method
- [ ] Add `getEndOffsetsForGroup()` helper method
- [ ] Test offset comparison logic

### Phase 4: Task Group Recreation Prevention
- [ ] Add `boundedOffsetsConsumed` flag
- [ ] Modify `createNewTasks()` to call `hasTaskGroupReachedBoundedEnd()` before skipping
- [ ] Only skip recreation if task group reached end offsets (completed successfully)
- [ ] Allow recreation if task group hasn't reached end offsets (failed midway)
- [ ] Set `boundedOffsetsConsumed=true` after first task group creation

### Phase 5: Offset Generation
- [ ] Modify `generateStartingSequencesForPartitionGroup()` to check bounded mode
- [ ] Add `generateBoundedStartingOffsets()` method
- [ ] Add fallback to bounded start offset if no checkpoint exists (for failed tasks)
- [ ] Add `extractBoundedEndOffsets()` method

### Phase 6: Task Creation
- [ ] Modify `TaskGroup` to include `endSequences`
- [ ] Update `createNewTasks()` to extract and pass end offsets
- [ ] Update `createTasksForGroup()` to use bounded end offsets
- [ ] Ensure task IOConfig receives correct start/end offsets

### Phase 7: Completion Handling
- [ ] Add `boundedTasksCompleted` flag
- [ ] Add `checkForBoundedCompletion()` method
- [ ] Add `handleBoundedCompletion()` method
- [ ] Modify `runInternal()` to check for bounded completion (before and after task status checks)

### Phase 8: Kafka/Kinesis Implementation
- [ ] Implement `isOffsetAtOrBeyond()` in `KafkaSupervisor`
- [ ] Implement `isOffsetAtOrBeyond()` in `KinesisSupervisor`
- [ ] Add `createBoundedCopy()` to `KafkaSupervisorSpec`
- [ ] Add `createBoundedCopy()` to `KinesisSupervisorSpec`
- [ ] Verify task creation uses correct end offsets

### Phase 9: resetOffsetsAndBackfill Integration
- [ ] Refactor `resetSupervisorAndBackfill()` to use bounded supervisors
- [ ] Add `createBoundedBackfillSpec()` method
- [ ] Update API response format
- [ ] Remove old backfill logic if any

### Phase 10: Testing
- [ ] Unit tests for `BoundedStreamConfig` validation
- [ ] Unit tests for `isOffsetAtOrBeyond()` implementations
- [ ] Unit tests for `hasTaskGroupReachedBoundedEnd()`
- [ ] Unit tests for offset generation in bounded mode (with and without checkpoints)
- [ ] Unit tests for partition initialization
- [ ] Unit tests for completion detection
- [ ] Integration test: bounded supervisor completes and terminates
- [ ] Integration test: bounded task fails midway, gets recreated, resumes from checkpoint
- [ ] Integration test: bounded task fails before checkpoint, restarts from beginning
- [ ] Integration test: resetOffsetsAndBackfill creates bounded supervisor
- [ ] Integration test: bounded and streaming supervisors coexist (concurrent locks)
- [ ] Integration test: supervisor restart during bounded task execution

### Phase 11: Documentation
- [ ] Update API documentation for bounded config
- [ ] Update supervisor documentation
- [ ] Update resetOffsetsAndBackfill API docs
- [ ] Add examples and tutorials

---

## 10. Edge Cases and Considerations

### Supervisor Restart
**Scenario**: Bounded supervisor restarts mid-processing

**Behavior**: 
- Partition discovery runs again, finds configured partitions
- Task discovery finds running tasks, adopts them
- Tasks continue from checkpointed offsets
- Supervisor continues monitoring until tasks complete

### Task Failure
**Scenario**: A bounded task fails

**Behavior**:
- Supervisor calls `hasTaskGroupReachedBoundedEnd()` to check completion
- If not complete (task failed midway): recreates task
- New task resumes from last checkpointed offset in metadata store
- If no checkpoint exists: falls back to bounded start offset
- Task continues processing until reaching end offset
- See [Section 6: Task Failure Handling](#6-task-failure-handling) for complete flow

### Invalid Partitions
**Scenario**: Configured partition doesn't exist in stream

**Behavior**:
- `updatePartitionDataForBoundedMode()` logs warning
- Only valid partitions are tracked
- If no valid partitions, supervisor fails to start

### Empty Offset Range
**Scenario**: Start offset equals end offset for a partition

**Behavior**:
- Task is created but immediately completes (no data to read)
- Supervisor detects completion normally
- No error, just completes quickly

### Concurrent Main + Backfill Supervisors
**Scenario**: Main supervisor running while backfill supervisor also running

**Behavior**:
- Both require `useConcurrentLocks=true`
- Both can write segments concurrently safely
- Backfill supervisor auto-terminates when done
- Main supervisor continues streaming normally

### Late Data in Backfill Range
**Scenario**: Backfill completes, but late data arrives in backfill range

**Behavior**:
- Backfill supervisor has already terminated
- Main supervisor (if still running) won't re-read that range
- User must manually handle if needed (re-run backfill or adjust main supervisor)

---

## 11. Future Enhancements

### Progress API
Add detailed progress tracking:
```
GET /druid/indexer/v1/supervisor/{supervisorId}/boundedProgress
```

### Bounded Supervisor Templates
Pre-configured bounded supervisor specs for common scenarios.

### Automatic Backfill Scheduling
Schedule backfill supervisors automatically based on lag metrics.

### Multi-Range Bounded Supervisors
Support multiple bounded ranges per supervisor (multiple backfill windows).

---

## Summary

This design enables bounded stream processing in Druid by:

1. **Adding configuration** for explicit start/end offsets via `BoundedStreamConfig`
2. **Initializing partitions from config** instead of stream discovery
3. **Preventing task recreation** by skipping task group creation after initial setup
4. **Leveraging existing task behavior** (tasks already auto-terminate at end offsets)
5. **Tracking completion** and optionally auto-terminating the supervisor
6. **Simplifying resetOffsetsAndBackfill** to just create a bounded supervisor

### Key Insights

1. **Tasks already handle bounded processing**
   - `SeekableStreamIndexTaskRunner` checks `isMoreToReadAfterReadingRecord()` 
   - When a partition reaches its `endSequenceNumber`, it's removed from assignment
   - When all partitions complete, the task exits naturally
   - We just need to configure tasks with proper start/end offsets

2. **The critical fix: Don't track partitions from stream**
   - Normal mode: `updatePartitionDataFromStream()` discovers all stream partitions
   - Problem in bounded mode: As tasks complete, supervisor sees "missing" partitions and recreates tasks
   - Solution: Initialize `partitionGroups` once from config, skip stream discovery
   - Additional safeguard: Check `boundedOffsetsConsumed` flag in `createNewTasks()`

3. **Failure handling: Distinguish completion from failure**
   - Problem: Tasks can complete successfully OR fail midway
   - Solution: `hasTaskGroupReachedBoundedEnd()` checks metadata storage offsets vs target end offsets
   - If task completed: skip recreation (offsets reached end)
   - If task failed: recreate task (offsets haven't reached end)
   - Retry resumes from last checkpoint, avoiding data loss and duplicate processing

4. **Three-part mechanism**
   - **Part 1**: `partitionGroups` populated from config during startup (not from stream)
   - **Part 2**: `createNewTasks()` checks `hasTaskGroupReachedBoundedEnd()` before skipping
   - **Part 3**: Failed tasks automatically recreated, successful tasks not recreated
   - This ensures: exactly-once task creation on success, automatic retry on failure

5. **Bounded supervisors use existing machinery**
   - Same supervisor class, same run loop
   - Just different initialization and task creation logic
   - Clean separation with `if (ioConfig.isBounded())` checks

### What Makes This Work

```
Normal Streaming Mode:
  updatePartitionDataFromStream() → discovers all partitions → populates partitionGroups
  createNewTasks() → loops through partitionGroups → creates tasks for uncovered partitions
  (repeats forever, tasks are recreated as they complete)

Bounded Mode (Success):
  initializeBoundedPartitionGroups() → populates from config (once at startup)
  updatePartitionDataFromStream() → skips or validates only (doesn't update partitionGroups)
  createNewTasks() → creates task groups once
  Tasks complete successfully → hasTaskGroupReachedBoundedEnd()=true → skips recreation
  All tasks done → supervisor detects completion → terminates

Bounded Mode (Failure):
  Task fails midway (offset=3000, end=5000)
  createNewTasks() → hasTaskGroupReachedBoundedEnd()=false (3000 < 5000)
  Recreates task → starts from checkpoint (3000) → processes remaining (3000-5000)
  Task completes → hasTaskGroupReachedBoundedEnd()=true (5000 >= 5000) → skips recreation
```

This results in a clean, maintainable implementation that:
- ✅ Reuses existing code and patterns
- ✅ Prevents infinite task recreation
- ✅ Handles task failures with automatic retry
- ✅ Avoids data loss through checkpoint-based recovery
- ✅ Prevents duplicate processing
