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

package org.apache.druid.indexing.rabbitstream.supervisor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RabbitStreamSupervisorBoundedModeTest
{
  @Test
  public void testCreatePartitionIdFromString()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    String partition = supervisor.createPartitionIdFromString("queue-0");

    Assert.assertEquals("queue-0", partition);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithInteger()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Long offset = supervisor.createSequenceOffsetFromObject(100);

    Assert.assertEquals(Long.valueOf(100L), offset);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithLong()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Long offset = supervisor.createSequenceOffsetFromObject(100L);

    Assert.assertEquals(Long.valueOf(100L), offset);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithString()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Long offset = supervisor.createSequenceOffsetFromObject("100");

    Assert.assertEquals(Long.valueOf(100L), offset);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithInvalidType()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    IllegalArgumentException ex = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> supervisor.createSequenceOffsetFromObject(new Object())
    );

    Assert.assertTrue(ex.getMessage().contains("Cannot convert"));
  }

  @Test
  public void testIsOffsetAtOrBeyondEqual()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Assert.assertTrue(supervisor.isOffsetAtOrBeyond(100L, 100L));
  }

  @Test
  public void testIsOffsetAtOrBeyondGreater()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Assert.assertTrue(supervisor.isOffsetAtOrBeyond(200L, 100L));
  }

  @Test
  public void testIsOffsetAtOrBeyondLess()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Assert.assertFalse(supervisor.isOffsetAtOrBeyond(50L, 100L));
  }

  @Test
  public void testConvertBoundedConfigMapWithIntegerValues()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Map<String, Integer> rawMap = new HashMap<>();
    rawMap.put("queue-0", 100);
    rawMap.put("queue-1", 200);

    Map<String, Long> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(2, converted.size());
    Assert.assertEquals(Long.valueOf(100L), converted.get("queue-0"));
    Assert.assertEquals(Long.valueOf(200L), converted.get("queue-1"));
  }

  @Test
  public void testConvertBoundedConfigMapWithStringValues()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Map<String, String> rawMap = new HashMap<>();
    rawMap.put("queue-0", "100");
    rawMap.put("queue-1", "200");

    Map<String, Long> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(2, converted.size());
    Assert.assertEquals(Long.valueOf(100L), converted.get("queue-0"));
    Assert.assertEquals(Long.valueOf(200L), converted.get("queue-1"));
  }

  @Test
  public void testConvertBoundedConfigMapWithMixedValues()
  {
    TestableRabbitStreamSupervisor supervisor = new TestableRabbitStreamSupervisor();

    Map<String, Object> rawMap = new HashMap<>();
    rawMap.put("queue-0", 100);
    rawMap.put("queue-1", "200");
    rawMap.put("queue-2", 300L);

    Map<String, Long> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(3, converted.size());
    Assert.assertEquals(Long.valueOf(100L), converted.get("queue-0"));
    Assert.assertEquals(Long.valueOf(200L), converted.get("queue-1"));
    Assert.assertEquals(Long.valueOf(300L), converted.get("queue-2"));
  }

  /**
   * Minimal testable subclass that exposes protected methods for testing.
   */
  private static class TestableRabbitStreamSupervisor extends RabbitStreamSupervisor
  {
    public TestableRabbitStreamSupervisor()
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
    public String createPartitionIdFromString(String partitionIdString)
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

    public Map<String, Long> convertBoundedConfigMap(Map<?, ?> rawMap)
    {
      Map<String, Long> result = new HashMap<>();
      for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
        String partition = createPartitionIdFromString(entry.getKey().toString());
        Long offset = createSequenceOffsetFromObject(entry.getValue());
        result.put(partition, offset);
      }
      return result;
    }
  }
}
