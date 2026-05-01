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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BoundedStreamConfigTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testConstructorWithValidMaps()
  {
    Map<String, Long> startOffsets = new HashMap<>();
    startOffsets.put("0", 100L);
    startOffsets.put("1", 200L);

    Map<String, Long> endOffsets = new HashMap<>();
    endOffsets.put("0", 500L);
    endOffsets.put("1", 600L);

    BoundedStreamConfig config = new BoundedStreamConfig(startOffsets, endOffsets);

    Assert.assertEquals(startOffsets, config.getStartSequenceNumbers());
    Assert.assertEquals(endOffsets, config.getEndSequenceNumbers());
  }

  @Test
  public void testConstructorWithNullStartSequenceNumbers()
  {
    Map<String, Long> endOffsets = new HashMap<>();
    endOffsets.put("0", 500L);

    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(null, endOffsets)
    );

    Assert.assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testConstructorWithNullEndSequenceNumbers()
  {
    Map<String, Long> startOffsets = new HashMap<>();
    startOffsets.put("0", 100L);

    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(startOffsets, null)
    );

    Assert.assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testConstructorWithEmptyStartSequenceNumbers()
  {
    Map<String, Long> startOffsets = new HashMap<>();
    Map<String, Long> endOffsets = new HashMap<>();
    endOffsets.put("0", 500L);

    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(startOffsets, endOffsets)
    );

    Assert.assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testConstructorWithEmptyEndSequenceNumbers()
  {
    Map<String, Long> startOffsets = new HashMap<>();
    startOffsets.put("0", 100L);
    Map<String, Long> endOffsets = new HashMap<>();

    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(startOffsets, endOffsets)
    );

    Assert.assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testConstructorWithMismatchedPartitions()
  {
    Map<String, Long> startOffsets = new HashMap<>();
    startOffsets.put("0", 100L);
    Map<String, Long> endOffsets = new HashMap<>();
    endOffsets.put("1", 500L);

    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(startOffsets, endOffsets)
    );

    Assert.assertTrue(ex.getMessage().contains("must have matching partition sets"));
  }

  @Test
  public void testSerializationDeserialization() throws Exception
  {
    Map<String, Integer> startOffsets = new HashMap<>();
    startOffsets.put("0", 100);
    startOffsets.put("1", 200);

    Map<String, Integer> endOffsets = new HashMap<>();
    endOffsets.put("0", 500);
    endOffsets.put("1", 600);

    BoundedStreamConfig config = new BoundedStreamConfig(startOffsets, endOffsets);

    String json = mapper.writeValueAsString(config);
    BoundedStreamConfig deserialized = mapper.readValue(json, BoundedStreamConfig.class);

    // Check sizes
    Assert.assertEquals(2, deserialized.getStartSequenceNumbers().size());
    Assert.assertEquals(2, deserialized.getEndSequenceNumbers().size());

    // Check that deserialized maps contain expected values (keys will be Strings after deserialization)
    Assert.assertEquals(100, deserialized.getStartSequenceNumbers().get("0"));
    Assert.assertEquals(200, deserialized.getStartSequenceNumbers().get("1"));
    Assert.assertEquals(500, deserialized.getEndSequenceNumbers().get("0"));
    Assert.assertEquals(600, deserialized.getEndSequenceNumbers().get("1"));
  }

  @Test
  public void testDeserializationWithIntegerValues() throws Exception
  {
    String json = "{"
                  + "\"startSequenceNumbers\": {\"0\": 100, \"1\": 200},"
                  + "\"endSequenceNumbers\": {\"0\": 500, \"1\": 600}"
                  + "}";

    BoundedStreamConfig config = mapper.readValue(json, BoundedStreamConfig.class);

    Assert.assertNotNull(config.getStartSequenceNumbers());
    Assert.assertNotNull(config.getEndSequenceNumbers());
    Assert.assertEquals(2, config.getStartSequenceNumbers().size());
    Assert.assertEquals(2, config.getEndSequenceNumbers().size());
  }

  @Test
  public void testDeserializationWithStringValues() throws Exception
  {
    String json = "{"
                  + "\"startSequenceNumbers\": {\"0\": \"100\", \"1\": \"200\"},"
                  + "\"endSequenceNumbers\": {\"0\": \"500\", \"1\": \"600\"}"
                  + "}";

    BoundedStreamConfig config = mapper.readValue(json, BoundedStreamConfig.class);

    Assert.assertNotNull(config.getStartSequenceNumbers());
    Assert.assertNotNull(config.getEndSequenceNumbers());
    Assert.assertEquals(2, config.getStartSequenceNumbers().size());
    Assert.assertEquals(2, config.getEndSequenceNumbers().size());
  }

  @Test
  public void testDeserializationWithMixedTypes() throws Exception
  {
    String json = "{"
                  + "\"startSequenceNumbers\": {\"0\": 100, \"1\": \"200\"},"
                  + "\"endSequenceNumbers\": {\"0\": 500, \"1\": \"600\"}"
                  + "}";

    BoundedStreamConfig config = mapper.readValue(json, BoundedStreamConfig.class);

    Assert.assertNotNull(config.getStartSequenceNumbers());
    Assert.assertNotNull(config.getEndSequenceNumbers());
    Assert.assertEquals(2, config.getStartSequenceNumbers().size());
    Assert.assertEquals(2, config.getEndSequenceNumbers().size());
  }
}
