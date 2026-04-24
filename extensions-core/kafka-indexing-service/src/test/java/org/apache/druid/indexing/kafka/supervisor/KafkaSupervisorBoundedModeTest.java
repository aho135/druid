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

package org.apache.druid.indexing.kafka.supervisor;

import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaSupervisorBoundedModeTest
{
  @Test
  public void testCreatePartitionIdFromString()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    KafkaTopicPartition partition = supervisor.createPartitionIdFromString("my-topic:5");

    Assert.assertEquals("my-topic", partition.topic());
    Assert.assertEquals(5, partition.partition());
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithInteger()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Long offset = supervisor.createSequenceOffsetFromObject(100);

    Assert.assertEquals(Long.valueOf(100L), offset);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithLong()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Long offset = supervisor.createSequenceOffsetFromObject(100L);

    Assert.assertEquals(Long.valueOf(100L), offset);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithString()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Long offset = supervisor.createSequenceOffsetFromObject("100");

    Assert.assertEquals(Long.valueOf(100L), offset);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithInvalidType()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    IllegalArgumentException ex = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> supervisor.createSequenceOffsetFromObject(new Object())
    );

    Assert.assertTrue(ex.getMessage().contains("Cannot convert"));
  }

  @Test
  public void testIsOffsetAtOrBeyondEqual()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Assert.assertTrue(supervisor.isOffsetAtOrBeyond(100L, 100L));
  }

  @Test
  public void testIsOffsetAtOrBeyondGreater()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Assert.assertTrue(supervisor.isOffsetAtOrBeyond(200L, 100L));
  }

  @Test
  public void testIsOffsetAtOrBeyondLess()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Assert.assertFalse(supervisor.isOffsetAtOrBeyond(50L, 100L));
  }

  @Test
  public void testConvertBoundedConfigMapWithIntegerValues()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Map<String, Integer> rawMap = new HashMap<>();
    rawMap.put("my-topic:0", 100);
    rawMap.put("my-topic:1", 200);

    Map<KafkaTopicPartition, Long> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(2, converted.size());
    Assert.assertEquals(Long.valueOf(100L), converted.get(new KafkaTopicPartition(false, "my-topic", 0)));
    Assert.assertEquals(Long.valueOf(200L), converted.get(new KafkaTopicPartition(false, "my-topic", 1)));
  }

  @Test
  public void testConvertBoundedConfigMapWithStringValues()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Map<String, String> rawMap = new HashMap<>();
    rawMap.put("my-topic:0", "100");
    rawMap.put("my-topic:1", "200");

    Map<KafkaTopicPartition, Long> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(2, converted.size());
    Assert.assertEquals(Long.valueOf(100L), converted.get(new KafkaTopicPartition(false, "my-topic", 0)));
    Assert.assertEquals(Long.valueOf(200L), converted.get(new KafkaTopicPartition(false, "my-topic", 1)));
  }

  @Test
  public void testConvertBoundedConfigMapWithMixedValues()
  {
    TestableKafkaSupervisor supervisor = new TestableKafkaSupervisor();

    Map<String, Object> rawMap = new HashMap<>();
    rawMap.put("my-topic:0", 100);
    rawMap.put("my-topic:1", "200");
    rawMap.put("my-topic:2", 300L);

    Map<KafkaTopicPartition, Long> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(3, converted.size());
    Assert.assertEquals(Long.valueOf(100L), converted.get(new KafkaTopicPartition(false, "my-topic", 0)));
    Assert.assertEquals(Long.valueOf(200L), converted.get(new KafkaTopicPartition(false, "my-topic", 1)));
    Assert.assertEquals(Long.valueOf(300L), converted.get(new KafkaTopicPartition(false, "my-topic", 2)));
  }

  /**
   * Minimal testable subclass that exposes protected methods for testing.
   */
  private static class TestableKafkaSupervisor extends KafkaSupervisor
  {
    public TestableKafkaSupervisor()
    {
      super(
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );
    }

    @Override
    public KafkaTopicPartition createPartitionIdFromString(String partitionIdString)
    {
      return super.createPartitionIdFromString(partitionIdString);
    }

    @Override
    public Long createSequenceOffsetFromObject(Object offsetObj)
    {
      return super.createSequenceOffsetFromObject(offsetObj);
    }

    @Override
    public boolean isOffsetAtOrBeyond(Long current, Long target)
    {
      return super.isOffsetAtOrBeyond(current, target);
    }

    public Map<KafkaTopicPartition, Long> convertBoundedConfigMap(Map<?, ?> rawMap)
    {
      Map<KafkaTopicPartition, Long> result = new HashMap<>();
      for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
        KafkaTopicPartition partition = createPartitionIdFromString(entry.getKey().toString());
        Long offset = createSequenceOffsetFromObject(entry.getValue());
        result.put(partition, offset);
      }
      return result;
    }
  }
}
