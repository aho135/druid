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

package org.apache.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class OrderByColumnSpec
{
  public enum Direction
  {
    ASCENDING,
    DESCENDING;

    /**
     * Maintain a map of the enum values so that we can just do a lookup and get a null if it doesn't exist instead
     * of an exception thrown.
     */
    private static final Map<String, Direction> STUPID_ENUM_MAP;
    static {
      final ImmutableMap.Builder<String, Direction> bob = ImmutableMap.builder();
      for (Direction direction : Direction.values()) {
        bob.put(direction.name(), direction);
      }
      STUPID_ENUM_MAP = bob.build();
    }

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static Direction fromString(String name)
    {
      final String upperName = StringUtils.toUpperCase(name);
      Direction direction = STUPID_ENUM_MAP.get(upperName);

      if (direction == null) {
        for (Direction dir : Direction.values()) {
          if (dir.name().startsWith(upperName)) {
            if (direction != null) {
              throw new ISE("Ambiguous directions[%s] and [%s]", direction, dir);
            }
            direction = dir;
          }
        }
      }

      return direction;
    }
  }

  public static final StringComparator DEFAULT_DIMENSION_ORDER = StringComparators.LEXICOGRAPHIC;

  private final String dimension;
  private final Direction direction;
  private final StringComparator dimensionComparator;

  @JsonCreator
  public OrderByColumnSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("direction") Direction direction,
      @JsonProperty("dimensionOrder") StringComparator dimensionComparator
  )
  {
    this.dimension = dimension;
    this.direction = direction == null ? Direction.ASCENDING : direction;
    this.dimensionComparator = dimensionComparator == null ? DEFAULT_DIMENSION_ORDER : dimensionComparator;
  }

  @JsonCreator
  public static OrderByColumnSpec fromString(String dimension)
  {
    return new OrderByColumnSpec(dimension, null, null);
  }

  public static OrderByColumnSpec asc(String dimension)
  {
    return new OrderByColumnSpec(dimension, Direction.ASCENDING, null);
  }

  public static List<OrderByColumnSpec> ascending(String... dimension)
  {
    return Lists.transform(
        Arrays.asList(dimension),
        new Function<>()
        {
          @Override
          public OrderByColumnSpec apply(@Nullable String input)
          {
            return asc(input);
          }
        }
    );
  }

  public static OrderByColumnSpec desc(String dimension)
  {
    return new OrderByColumnSpec(dimension, Direction.DESCENDING, null);
  }

  public static List<OrderByColumnSpec> descending(String... dimension)
  {
    return Lists.transform(
        Arrays.asList(dimension),
        new Function<>()
        {
          @Override
          public OrderByColumnSpec apply(@Nullable String input)
          {
            return desc(input);
          }
        }
    );
  }

  public static OrderByColumnSpec getOrderByForDimName(List<OrderByColumnSpec> orderBys, String dimName)
  {
    for (OrderByColumnSpec orderBy : orderBys) {
      if (orderBy.dimension.equals(dimName)) {
        return orderBy;
      }
    }
    return null;
  }

  public static int getDimIndexForOrderBy(OrderByColumnSpec orderSpec, List<DimensionSpec> dimensions)
  {
    int i = 0;
    for (DimensionSpec dimSpec : dimensions) {
      if (orderSpec.getDimension().equals((dimSpec.getOutputName()))) {
        return i;
      }
      i++;
    }
    return -1;
  }

  public static int getAggIndexForOrderBy(OrderByColumnSpec orderSpec, List<AggregatorFactory> aggregatorFactories)
  {
    int i = 0;
    for (AggregatorFactory agg : aggregatorFactories) {
      if (orderSpec.getDimension().equals((agg.getName()))) {
        return i;
      }
      i++;
    }
    return -1;
  }

  public static int getPostAggIndexForOrderBy(OrderByColumnSpec orderSpec, List<PostAggregator> postAggs)
  {
    int i = 0;
    for (PostAggregator postAgg : postAggs) {
      if (orderSpec.getDimension().equals((postAgg.getName()))) {
        return i;
      }
      i++;
    }
    return -1;
  }

  public OrderByColumnSpec(String dimension, Direction direction)
  {
    this(dimension, direction, null);
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty("direction")
  public Direction getDirection()
  {
    return direction;
  }

  @JsonProperty("dimensionOrder")
  public StringComparator getDimensionComparator()
  {
    return dimensionComparator;
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

    OrderByColumnSpec that = (OrderByColumnSpec) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!dimensionComparator.equals(that.dimensionComparator)) {
      return false;
    }
    return direction == that.direction;

  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + direction.hashCode();
    result = 31 * result + dimensionComparator.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "OrderByColumnSpec{" +
           "dimension='" + dimension + '\'' +
           ", direction='" + direction + '\'' +
           ", dimensionComparator='" + dimensionComparator + '\'' +
           '}';
  }

  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] directionBytes = StringUtils.toUtf8(direction.name());

    return ByteBuffer.allocate(dimensionBytes.length + directionBytes.length)
                     .put(dimensionBytes)
                     .put(directionBytes)
                     .array();
  }
}
