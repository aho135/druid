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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.segment.CompleteSegment;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.sql.calcite.CalciteNestedDataQueryTest;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.druid.sql.calcite.util.CalciteTests.ARRAYS_DATASOURCE;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE1;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE2;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE3;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE5;
import static org.apache.druid.sql.calcite.util.CalciteTests.WIKIPEDIA;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.INDEX_SCHEMA_LOTS_O_COLUMNS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.INDEX_SCHEMA_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS2;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS_LOTS_OF_COLUMNS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

/**
 * Helper class aiding in wiring up the Guice bindings required for MSQ engine to work with the Calcite's tests
 */
public class CalciteMSQTestsHelper
{
  public static final class MSQTestModule implements DruidModule
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bind(AppenderatorsManager.class).toProvider(() -> null);

      // Requirements of JoinableFactoryModule
      binder.bind(SegmentManager.class).toInstance(EasyMock.createMock(SegmentManager.class));

      binder.bind(new TypeLiteral<Set<NodeRole>>()
      {
      }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));

      DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
      {
        @Override
        public String getFormatString()
        {
          return "test";
        }
      };
      binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
      binder.bind(QueryProcessingPool.class)
            .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool")));

      binder.bind(Bouncer.class).toInstance(new Bouncer(1));
    }

    @Provides
    public SegmentCacheManager provideSegmentCacheManager(ObjectMapper testMapper, TempDirProducer tempDirProducer)
    {
      return new SegmentCacheManagerFactory(TestIndex.INDEX_IO, testMapper)
          .manufacturate(tempDirProducer.newTempFolder("test"));
    }

    @Provides
    public LocalDataSegmentPusherConfig provideLocalDataSegmentPusherConfig(TempDirProducer tempDirProducer)
    {
      LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
      config.storageDirectory = tempDirProducer.newTempFolder("localsegments");
      return config;
    }

    @Provides
    public MSQTestSegmentManager provideMSQTestSegmentManager(SegmentCacheManager segmentCacheManager)
    {
      return new MSQTestSegmentManager(segmentCacheManager);
    }

    @Provides
    public DataSegmentPusher provideDataSegmentPusher(LocalDataSegmentPusherConfig config,
        MSQTestSegmentManager segmentManager)
    {
      return new MSQTestDelegateDataSegmentPusher(new LocalDataSegmentPusher(config), segmentManager);
    }

    @Provides
    public DataSegmentAnnouncer provideDataSegmentAnnouncer()
    {
      return new NoopDataSegmentAnnouncer();
    }

    @Provides
    @LazySingleton
    public DataSegmentProvider provideDataSegmentProvider(LocalDataSegmentProvider localDataSegmentProvider)
    {
      return localDataSegmentProvider;
    }

    @LazySingleton
    static class LocalDataSegmentProvider extends CacheLoader<SegmentId, CompleteSegment> implements DataSegmentProvider
    {
      private TempDirProducer tempDirProducer;
      private LoadingCache<SegmentId, CompleteSegment> cache;

      @Inject
      public LocalDataSegmentProvider(TempDirProducer tempDirProducer)
      {
        this.tempDirProducer = tempDirProducer;
        this.cache = CacheBuilder.newBuilder().build(this);
      }

      @Override
      public CompleteSegment load(SegmentId segmentId) throws Exception
      {
        return getSupplierForSegment(tempDirProducer::newTempFolder, segmentId);
      }

      @Override
      public Supplier<ResourceHolder<CompleteSegment>> fetchSegment(SegmentId segmentId,
          ChannelCounters channelCounters, boolean isReindex)
      {
        CompleteSegment a = cache.getUnchecked(segmentId);
        return () -> new ReferenceCountingResourceHolder<>(a, Closer.create());
      }

    }

    @Provides
    public DataServerQueryHandlerFactory provideDataServerQueryHandlerFactory()
    {
      return getTestDataServerQueryHandlerFactory();
    }

    @Provides
    @LazySingleton
    GroupingEngine getGroupingEngine(GroupByQueryConfig groupByQueryConfig, TestGroupByBuffers groupByBuffers)
    {
      GroupingEngine groupingEngine = GroupByQueryRunnerTest.makeQueryRunnerFactory(
          groupByQueryConfig,
          groupByBuffers
      ).getGroupingEngine();
      return groupingEngine;
    }

  }

  @Deprecated
  public static List<Module> fetchModules(
      Function<String, File> tempFolderProducer,
      TestGroupByBuffers groupByBuffers
  )
  {
    return ImmutableList.of(
        new MSQTestModule(),
        new IndexingServiceTuningConfigModule(),
        new JoinableFactoryModule(),
        new MSQExternalDataSourceModule(),
        new MSQIndexingModule()
    );
  }

  private static DataServerQueryHandlerFactory getTestDataServerQueryHandlerFactory()
  {
    // Currently, there is no metadata in this test for loaded segments. Therefore, this should not be called.
    // In the future, if this needs to be supported, mocks for DataServerQueryHandler should be added like
    // org.apache.druid.msq.exec.MSQLoadedSegmentTests.
    DataServerQueryHandlerFactory mockFactory = Mockito.mock(DataServerQueryHandlerFactory.class);
    DataServerQueryHandler dataServerQueryHandler = Mockito.mock(DataServerQueryHandler.class);
    doThrow(new AssertionError("Test does not support loaded segment query"))
        .when(dataServerQueryHandler).fetchRowsFromDataServer(any(), any(), any());
    doReturn(dataServerQueryHandler).when(mockFactory).createDataServerQueryHandler(anyString(), any(), any());
    return mockFactory;
  }

  protected static CompleteSegment getSupplierForSegment(
      Function<String, File> tempFolderProducer,
      SegmentId segmentId
  )
  {
    final QueryableIndex index;
    switch (segmentId.getDataSource()) {
      case WIKIPEDIA:
        try {
          final File directory = new File(tempFolderProducer.apply("tmpDir"), StringUtils.format("wikipedia-index-%s", UUID.randomUUID()));
          final IncrementalIndex incrementalIndex = TestIndex.makeWikipediaIncrementalIndex();
          TestIndex.INDEX_MERGER.persist(incrementalIndex, directory, IndexSpec.DEFAULT, null);
          index = TestIndex.INDEX_IO.loadIndex(directory);
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
        break;
      case DATASOURCE1:
        IncrementalIndexSchema foo1Schema = new IncrementalIndexSchema.Builder()
            .withMetrics(
                new CountAggregatorFactory("cnt"),
                new FloatSumAggregatorFactory("m1", "m1"),
                new DoubleSumAggregatorFactory("m2", "m2"),
                new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
            )
            .withRollup(false)
            .build();
        index = IndexBuilder
            .create()
            .tmpDir(tempFolderProducer.apply("tmpDir"))
            .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
            .schema(foo1Schema)
            .rows(ROWS1)
            .buildMMappedIndex();
        break;
      case DATASOURCE2:
        final IncrementalIndexSchema indexSchemaDifferentDim3M1Types = new IncrementalIndexSchema.Builder()
            .withDimensionsSpec(
                new DimensionsSpec(
                    ImmutableList.of(
                        new StringDimensionSchema("dim1"),
                        new StringDimensionSchema("dim2"),
                        new LongDimensionSchema("dim3")
                    )
                )
            )
            .withMetrics(
                new CountAggregatorFactory("cnt"),
                new LongSumAggregatorFactory("m1", "m1"),
                new DoubleSumAggregatorFactory("m2", "m2"),
                new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
            )
            .withRollup(false)
            .build();
        index = IndexBuilder
            .create()
            .tmpDir(tempFolderProducer.apply("tmpDir"))
            .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
            .schema(indexSchemaDifferentDim3M1Types)
            .rows(ROWS2)
            .buildMMappedIndex();
        break;
      case DATASOURCE3:
      case CalciteTests.BROADCAST_DATASOURCE:
        index = IndexBuilder
            .create()
            .tmpDir(tempFolderProducer.apply("tmpDir"))
            .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
            .schema(INDEX_SCHEMA_NUMERIC_DIMS)
            .rows(ROWS1_WITH_NUMERIC_DIMS)
            .buildMMappedIndex();
        break;
      case DATASOURCE5:
        index = IndexBuilder
            .create()
            .tmpDir(tempFolderProducer.apply("tmpDir"))
            .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
            .schema(INDEX_SCHEMA_LOTS_O_COLUMNS)
            .rows(ROWS_LOTS_OF_COLUMNS)
            .buildMMappedIndex();
        break;
      case ARRAYS_DATASOURCE:
        index = IndexBuilder.create()
                            .tmpDir(tempFolderProducer.apply("tmpDir"))
                            .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                            .schema(
                                new IncrementalIndexSchema.Builder()
                                    .withTimestampSpec(NestedDataTestUtils.AUTO_SCHEMA.getTimestampSpec())
                                    .withDimensionsSpec(NestedDataTestUtils.AUTO_SCHEMA.getDimensionsSpec())
                                    .withMetrics(
                                        new CountAggregatorFactory("cnt")
                                    )
                                    .withRollup(false)
                                    .build()
                            )
                            .inputSource(
                                ResourceInputSource.of(
                                    NestedDataTestUtils.class.getClassLoader(),
                                    NestedDataTestUtils.ARRAY_TYPES_DATA_FILE
                                )
                            )
                            .inputFormat(TestDataBuilder.DEFAULT_JSON_INPUT_FORMAT)
                            .inputTmpDir(tempFolderProducer.apply("tmpDir"))
                            .buildMMappedIndex();
        break;
      case CalciteNestedDataQueryTest.DATA_SOURCE:
      case CalciteNestedDataQueryTest.DATA_SOURCE_MIXED:
        if (segmentId.getPartitionNum() == 0) {
          index = IndexBuilder.create()
                              .tmpDir(tempFolderProducer.apply("tmpDir"))
                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                              .schema(
                                  new IncrementalIndexSchema.Builder()
                                      .withMetrics(
                                          new CountAggregatorFactory("cnt")
                                      )
                                      .withDimensionsSpec(CalciteNestedDataQueryTest.ALL_JSON_COLUMNS.getDimensionsSpec())
                                      .withRollup(false)
                                      .build()
                              )
                              .rows(CalciteNestedDataQueryTest.ROWS)
                              .buildMMappedIndex();
        } else if (segmentId.getPartitionNum() == 1) {
          index = IndexBuilder.create()
                              .tmpDir(tempFolderProducer.apply("tmpDir"))
                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                              .schema(
                                  new IncrementalIndexSchema.Builder()
                                      .withMetrics(
                                          new CountAggregatorFactory("cnt")
                                      )
                                      .withDimensionsSpec(CalciteNestedDataQueryTest.JSON_AND_SCALAR_MIX.getDimensionsSpec())
                                      .withRollup(false)
                                      .build()
                              )
                              .rows(CalciteNestedDataQueryTest.ROWS_MIX)
                              .buildMMappedIndex();
        } else {
          throw new ISE("Cannot query segment %s in test runner", segmentId);
        }
        break;
      case CalciteNestedDataQueryTest.DATA_SOURCE_MIXED_2:
        if (segmentId.getPartitionNum() == 0) {
          index = IndexBuilder.create()
                              .tmpDir(tempFolderProducer.apply("tmpDir"))
                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                              .schema(
                                  new IncrementalIndexSchema.Builder()
                                      .withMetrics(
                                          new CountAggregatorFactory("cnt")
                                      )
                                      .withDimensionsSpec(CalciteNestedDataQueryTest.JSON_AND_SCALAR_MIX.getDimensionsSpec())
                                      .withRollup(false)
                                      .build()
                              )
                              .rows(CalciteNestedDataQueryTest.ROWS_MIX)
                              .buildMMappedIndex();
        } else if (segmentId.getPartitionNum() == 1) {
          index = IndexBuilder.create()
                      .tmpDir(tempFolderProducer.apply("tmpDir"))
                      .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                      .schema(
                          new IncrementalIndexSchema.Builder()
                              .withMetrics(
                                  new CountAggregatorFactory("cnt")
                              )
                              .withDimensionsSpec(CalciteNestedDataQueryTest.ALL_JSON_COLUMNS.getDimensionsSpec())
                              .withRollup(false)
                              .build()
                      )
                      .rows(CalciteNestedDataQueryTest.ROWS)
                      .buildMMappedIndex();
        } else {
          throw new ISE("Cannot query segment %s in test runner", segmentId);
        }
        break;
      case CalciteNestedDataQueryTest.DATA_SOURCE_ALL:
        index = IndexBuilder.create()
                            .tmpDir(tempFolderProducer.apply("tmpDir"))
                            .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                            .schema(
                                new IncrementalIndexSchema.Builder()
                                    .withTimestampSpec(NestedDataTestUtils.AUTO_SCHEMA.getTimestampSpec())
                                    .withDimensionsSpec(NestedDataTestUtils.AUTO_SCHEMA.getDimensionsSpec())
                                    .withMetrics(
                                        new CountAggregatorFactory("cnt")
                                    )
                                    .withRollup(false)
                                    .build()
                            )
                            .inputSource(
                                ResourceInputSource.of(
                                    NestedDataTestUtils.class.getClassLoader(),
                                    NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE
                                )
                            )
                            .inputFormat(TestDataBuilder.DEFAULT_JSON_INPUT_FORMAT)
                            .inputTmpDir(tempFolderProducer.apply("tmpDir"))
                            .buildMMappedIndex();
        break;
      case CalciteTests.WIKIPEDIA_FIRST_LAST:
        index = TestDataBuilder.makeWikipediaIndexWithAggregation(tempFolderProducer.apply("tmpDir"));
        break;
      case CalciteTests.TBL_WITH_NULLS_PARQUET:
      case CalciteTests.SML_TBL_PARQUET:
      case CalciteTests.ALL_TYPES_UNIQ_PARQUET:
      case CalciteTests.FEW_ROWS_ALL_DATA_PARQUET:
      case CalciteTests.T_ALL_TYPE_PARQUET:
        index = TestDataBuilder.getQueryableIndexForDrillDatasource(segmentId.getDataSource(), tempFolderProducer.apply("tmpDir"));
        break;
      case CalciteTests.BENCHMARK_DATASOURCE:
        index = TestDataBuilder.getQueryableIndexForBenchmarkDatasource();
        break;
      default:
        throw new ISE("Cannot query segment %s in test runner", segmentId);

    }
    Segment segment = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return segmentId;
      }

      @Override
      public Interval getDataInterval()
      {
        return segmentId.getInterval();
      }

      @Nullable
      @Override
      public QueryableIndex asQueryableIndex()
      {
        return index;
      }

      @Override
      public CursorFactory asCursorFactory()
      {
        return new QueryableIndexCursorFactory(index);
      }

      @Override
      public void close()
      {
      }
    };
    DataSegment dataSegment = DataSegment.builder()
                                         .dataSource(segmentId.getDataSource())
                                         .interval(segmentId.getInterval())
                                         .version(segmentId.getVersion())
                                         .shardSpec(new LinearShardSpec(0))
                                         .size(0)
                                         .build();
    return new CompleteSegment(dataSegment, segment);
  }
}
