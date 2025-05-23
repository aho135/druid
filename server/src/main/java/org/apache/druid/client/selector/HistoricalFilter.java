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

package org.apache.druid.client.selector;

import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.query.CloneQueryMode;

import java.util.Set;

/**
 * Interface that denotes some sort of filtering on the historcals, based on {@link CloneQueryMode}.
 */
public interface HistoricalFilter
{
  /**
   * Perform no filtering, regardless of the query mode.
   */
  HistoricalFilter IDENTITY_FILTER = (historicalServers, mode) -> historicalServers;

  /**
   * Returns a {@link Int2ObjectRBTreeMap} after performing a filtering on the {@link QueryableDruidServer}, based
   * on the cloneQueryMode paramter. The map in the parameter is not modified.
   */
  Int2ObjectRBTreeMap<Set<QueryableDruidServer>> getQueryableServers(
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers,
      CloneQueryMode mode
  );
}
