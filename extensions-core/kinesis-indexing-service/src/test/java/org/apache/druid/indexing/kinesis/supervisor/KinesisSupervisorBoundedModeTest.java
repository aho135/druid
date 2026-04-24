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

package org.apache.druid.indexing.kinesis.supervisor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KinesisSupervisorBoundedModeTest
{
  @Test
  public void testCreatePartitionIdFromString()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    String partition = supervisor.createPartitionIdFromString("shardId-000000000000");

    Assert.assertEquals("shardId-000000000000", partition);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithString()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    String offset = supervisor.createSequenceOffsetFromObject("49590338271490256608559692538361571095921575989136588898");

    Assert.assertEquals("49590338271490256608559692538361571095921575989136588898", offset);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithInteger()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    String offset = supervisor.createSequenceOffsetFromObject(100);

    Assert.assertEquals("100", offset);
  }

  @Test
  public void testCreateSequenceOffsetFromObjectWithLong()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    String offset = supervisor.createSequenceOffsetFromObject(100L);

    Assert.assertEquals("100", offset);
  }

  @Test
  public void testIsOffsetAtOrBeyondEqual()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    Assert.assertTrue(supervisor.isOffsetAtOrBeyond("49590338271490256608559692538361571095921575989136588898", "49590338271490256608559692538361571095921575989136588898"));
  }

  @Test
  public void testIsOffsetAtOrBeyondGreater()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    Assert.assertTrue(supervisor.isOffsetAtOrBeyond("49590338271512257353759162668991891722121171891717232706", "49590338271490256608559692538361571095921575989136588898"));
  }

  @Test
  public void testIsOffsetAtOrBeyondLess()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    Assert.assertFalse(supervisor.isOffsetAtOrBeyond("49590338271490256608559692538361571095921575989136588898", "49590338271512257353759162668991891722121171891717232706"));
  }

  @Test
  public void testIsOffsetAtOrBeyondWithSimpleStrings()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    Assert.assertTrue(supervisor.isOffsetAtOrBeyond("200", "100"));
    Assert.assertFalse(supervisor.isOffsetAtOrBeyond("100", "200"));
    Assert.assertTrue(supervisor.isOffsetAtOrBeyond("100", "100"));
  }

  @Test
  public void testConvertBoundedConfigMapWithStringValues()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    Map<String, String> rawMap = new HashMap<>();
    rawMap.put("shardId-000000000000", "49590338271490256608559692538361571095921575989136588898");
    rawMap.put("shardId-000000000001", "49590338271512257353759162668991891722121171891717232706");

    Map<String, String> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(2, converted.size());
    Assert.assertEquals("49590338271490256608559692538361571095921575989136588898", converted.get("shardId-000000000000"));
    Assert.assertEquals("49590338271512257353759162668991891722121171891717232706", converted.get("shardId-000000000001"));
  }

  @Test
  public void testConvertBoundedConfigMapWithNumericValues()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    Map<String, Object> rawMap = new HashMap<>();
    rawMap.put("shardId-000000000000", 100);
    rawMap.put("shardId-000000000001", 200L);

    Map<String, String> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(2, converted.size());
    Assert.assertEquals("100", converted.get("shardId-000000000000"));
    Assert.assertEquals("200", converted.get("shardId-000000000001"));
  }

  @Test
  public void testConvertBoundedConfigMapWithMixedValues()
  {
    TestableKinesisSupervisor supervisor = new TestableKinesisSupervisor();

    Map<String, Object> rawMap = new HashMap<>();
    rawMap.put("shardId-000000000000", "49590338271490256608559692538361571095921575989136588898");
    rawMap.put("shardId-000000000001", 100);
    rawMap.put("shardId-000000000002", 200L);

    Map<String, String> converted = supervisor.convertBoundedConfigMap(rawMap);

    Assert.assertEquals(3, converted.size());
    Assert.assertEquals("49590338271490256608559692538361571095921575989136588898", converted.get("shardId-000000000000"));
    Assert.assertEquals("100", converted.get("shardId-000000000001"));
    Assert.assertEquals("200", converted.get("shardId-000000000002"));
  }

  /**
   * Minimal testable subclass that exposes protected methods for testing.
   */
  private static class TestableKinesisSupervisor extends KinesisSupervisor
  {
    public TestableKinesisSupervisor()
    {
      super(
          null,
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
    public String createSequenceOffsetFromObject(Object offsetObj)
    {
      return super.createSequenceOffsetFromObject(offsetObj);
    }

    @Override
    public boolean isOffsetAtOrBeyond(String current, String target)
    {
      return super.isOffsetAtOrBeyond(current, target);
    }

    public Map<String, String> convertBoundedConfigMap(Map<?, ?> rawMap)
    {
      Map<String, String> result = new HashMap<>();
      for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
        String partition = createPartitionIdFromString(entry.getKey().toString());
        String offset = createSequenceOffsetFromObject(entry.getValue());
        result.put(partition, offset);
      }
      return result;
    }
  }
}
