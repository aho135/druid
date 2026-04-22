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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for bounded (one-time) stream processing with explicit start/end offsets.
 * <p>
 * When configured, the supervisor will:
 * <ol>
 *   <li>Create tasks starting at the specified startSequenceNumbers</li>
 *   <li>Tasks will automatically stop when they reach endSequenceNumbers</li>
 *   <li>Supervisor will not recreate tasks after they complete successfully</li>
 *   <li>Failed tasks are automatically recreated and resume from last checkpoint</li>
 *   <li>Optionally auto-terminate when all tasks are done</li>
 * </ol>
 * <p>
 * This is useful for:
 * <ul>
 *   <li>Backfill processing (via resetOffsetsAndBackfill)</li>
 *   <li>Historical reprocessing</li>
 *   <li>One-time migration tasks</li>
 * </ul>
 */
public class BoundedStreamConfig
{
  private final Map<?, ?> startSequenceNumbers;
  private final Map<?, ?> endSequenceNumbers;
  private final boolean terminateOnCompletion;
  @Nullable
  private final Integer taskCount;

  @JsonCreator
  public BoundedStreamConfig(
      @JsonProperty("startSequenceNumbers") Map<?, ?> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers") Map<?, ?> endSequenceNumbers,
      @JsonProperty("terminateOnCompletion") @Nullable Boolean terminateOnCompletion,
      @JsonProperty("taskCount") @Nullable Integer taskCount
  )
  {
    this.startSequenceNumbers = Preconditions.checkNotNull(startSequenceNumbers, "startSequenceNumbers cannot be null");
    this.endSequenceNumbers = Preconditions.checkNotNull(endSequenceNumbers, "endSequenceNumbers cannot be null");
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

    if (taskCount != null) {
      Preconditions.checkArgument(
          taskCount > 0,
          "taskCount must be positive, got: %s",
          taskCount
      );
    }
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BoundedStreamConfig that = (BoundedStreamConfig) o;
    return terminateOnCompletion == that.terminateOnCompletion
           && Objects.equals(startSequenceNumbers, that.startSequenceNumbers)
           && Objects.equals(endSequenceNumbers, that.endSequenceNumbers)
           && Objects.equals(taskCount, that.taskCount);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(startSequenceNumbers, endSequenceNumbers, terminateOnCompletion, taskCount);
  }

  @Override
  public String toString()
  {
    return "BoundedStreamConfig{" +
           "startSequenceNumbers=" + startSequenceNumbers +
           ", endSequenceNumbers=" + endSequenceNumbers +
           ", terminateOnCompletion=" + terminateOnCompletion +
           ", taskCount=" + taskCount +
           '}';
  }
}
