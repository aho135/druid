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

package org.apache.druid.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.QueryUtils;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DruidRel that uses a {@link JoinDataSource}.
 */
public class DruidJoinQueryRel extends DruidRel<DruidJoinQueryRel>
{
  static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__join__")
  {
    @Override
    public boolean isProcessable()
    {
      return false;
    }
  };

  private final Filter leftFilter;
  private final PartialDruidQuery partialQuery;
  private final Join joinRel;
  private final PlannerConfig plannerConfig;
  private RelNode left;
  private RelNode right;

  private DruidJoinQueryRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      Join joinRel,
      Filter leftFilter,
      PartialDruidQuery partialQuery,
      PlannerContext plannerContext
  )
  {
    super(cluster, traitSet, plannerContext);
    this.joinRel = joinRel;
    this.left = joinRel.getLeft();
    this.right = joinRel.getRight();
    this.leftFilter = leftFilter;
    this.partialQuery = partialQuery;
    this.plannerConfig = plannerContext.getPlannerConfig();
  }

  /**
   * Create an instance from a Join that is based on two {@link DruidRel} inputs.
   */
  public static DruidJoinQueryRel create(
      final Join joinRel,
      final Filter leftFilter,
      final PlannerContext plannerContext
  )
  {
    return new DruidJoinQueryRel(
        joinRel.getCluster(),
        joinRel.getTraitSet(),
        joinRel,
        leftFilter,
        PartialDruidQuery.create(joinRel),
        plannerContext
    );
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidJoinQueryRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidJoinQueryRel(
        getCluster(),
        newQueryBuilder.getTraitSet(getConvention(), getPlannerContext()),
        joinRel,
        leftFilter,
        newQueryBuilder,
        getPlannerContext()
    );
  }

  private SourceDesc buildLeftSourceDesc()
  {
    final SourceDesc leftDesc;
    final DruidRel<?> leftDruidRel = (DruidRel<?>) left;
    final DruidQuery leftQuery = Preconditions.checkNotNull(leftDruidRel.toDruidQuery(false), "leftQuery");
    final RowSignature leftSignature = leftQuery.getOutputRowSignature();
    final DataSource leftDataSource;
    if (computeLeftRequiresSubquery(getPlannerContext(), leftDruidRel, joinRel)) {
      leftDataSource = new QueryDataSource(leftQuery.getQuery());
      if (leftFilter != null) {
        throw new ISE("Filter on left table is supposed to be null if left child is a query source");
      }
    } else {
      leftDataSource = leftQuery.getDataSource();
    }
    leftDesc = new SourceDesc(leftDataSource, leftSignature);
    return leftDesc;
  }

  private SourceDesc buildRightSourceDesc()
  {
    final SourceDesc rightDesc;
    final DruidRel<?> rightDruidRel = (DruidRel<?>) right;
    final DruidQuery rightQuery = Preconditions.checkNotNull(rightDruidRel.toDruidQuery(false), "rightQuery");
    final RowSignature rightSignature = rightQuery.getOutputRowSignature();
    final DataSource rightDataSource;
    if (computeRightRequiresSubquery(getPlannerContext(), rightDruidRel)) {
      rightDataSource = new QueryDataSource(rightQuery.getQuery());
    } else {
      rightDataSource = rightQuery.getDataSource();
    }
    rightDesc = new SourceDesc(rightDataSource, rightSignature);
    return rightDesc;
  }

  public static SourceDesc buildJoinSourceDesc(final SourceDesc leftDesc, final SourceDesc rightDesc, PlannerContext plannerContext, Join joinRel, Filter leftFilter)
  {
    final Pair<String, RowSignature> prefixSignaturePair = computeJoinRowSignature(
        leftDesc.rowSignature,
        rightDesc.rowSignature,
        findExistingJoinPrefixes(leftDesc.dataSource, rightDesc.dataSource)
    );

    String prefix = prefixSignaturePair.lhs;
    RowSignature signature = prefixSignaturePair.rhs;

    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        signature,
        plannerContext.getExpressionParser(),
        plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
    );
    plannerContext.setJoinExpressionVirtualColumnRegistry(virtualColumnRegistry);

    // Generate the condition for this join as a Druid expression.
    final DruidExpression condition = Expressions.toDruidExpression(
        plannerContext,
        signature,
        joinRel.getCondition()
    );

    // Unsetting it to avoid any VC Registry leaks incase there are multiple druid quries for the SQL
    // It should be fixed soon with changes in interface for SqlOperatorConversion and Expressions bridge class
    plannerContext.setJoinExpressionVirtualColumnRegistry(null);

    // DruidJoinRule should not have created us if "condition" is null. Check defensively anyway, which also
    // quiets static code analysis.
    if (condition == null) {
      throw new CannotBuildQueryException(joinRel, joinRel.getCondition());
    }

    JoinDataSource joinDataSource = JoinDataSource.create(
        leftDesc.dataSource,
        rightDesc.dataSource,
        prefix,
        JoinConditionAnalysis.forExpression(
            condition.getExpression(),
            plannerContext.parseExpression(condition.getExpression()),
            prefix
        ),
        toDruidJoinType(joinRel.getJoinType()),
        getDimFilter(plannerContext, leftDesc.rowSignature, leftFilter),
        plannerContext.getJoinableFactoryWrapper(),
        QueryUtils.getJoinAlgorithm(joinRel, plannerContext)
    );

    SourceDesc sourceDesc = new SourceDesc(joinDataSource, signature, virtualColumnRegistry);
    return sourceDesc;
  }


  @Override
  public DruidQuery toDruidQuery(final boolean finalizeAggregations)
  {
    final SourceDesc leftDesc = buildLeftSourceDesc();
    final SourceDesc rightDesc = buildRightSourceDesc();

    SourceDesc sourceDesc = buildJoinSourceDesc(leftDesc, rightDesc, getPlannerContext(), joinRel, leftFilter);

    return partialQuery.build(
        sourceDesc.dataSource,
        sourceDesc.rowSignature,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations,
        true,
        sourceDesc.virtualColumnRegistry
    );
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            joinRel.getRowType().getFieldNames(),
            joinRel.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false,
        false
    );
  }

  @Override
  public DruidJoinQueryRel asDruidConvention()
  {
    return new DruidJoinQueryRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        joinRel.copy(
            joinRel.getTraitSet(),
            joinRel.getInputs()
                   .stream()
                   .map(input -> RelOptRule.convert(input, DruidConvention.instance()))
                   .collect(Collectors.toList())
        ),
        leftFilter,
        partialQuery,
        getPlannerContext()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    joinRel.replaceInput(ordinalInParent, p);

    if (ordinalInParent == 0) {
      this.left = p;
    } else if (ordinalInParent == 1) {
      this.right = p;
    } else {
      throw new IndexOutOfBoundsException(StringUtils.format("Invalid ordinalInParent[%s]", ordinalInParent));
    }
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidJoinQueryRel(
        getCluster(),
        traitSet,
        joinRel.copy(joinRel.getTraitSet(), inputs),
        leftFilter,
        getPartialDruidQuery(),
        getPlannerContext()
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    final Set<String> retVal = new HashSet<>();
    retVal.addAll(((DruidRel<?>) left).getDataSourceNames());
    retVal.addAll(((DruidRel<?>) right).getDataSourceNames());
    return retVal;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getPlannerContext().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return joinRel.explainTerms(pw)
                  .item("query", queryString)
                  .item("signature", druidQuery.getOutputRowSignature());
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    double joinCost = partialQuery.estimateCost();

    if (getPlannerContext().getJoinAlgorithm().requiresSubquery()) {
      joinCost *= CostEstimates.MULTIPLIER_OUTER_QUERY;
    } else {
      // Penalize subqueries if we don't have to do them.
      if (computeLeftRequiresSubquery(getPlannerContext(), getSomeDruidChild(left), joinRel)) {
        joinCost += CostEstimates.COST_SUBQUERY;
      } else {
        if (joinRel.getJoinType() == JoinRelType.INNER && plannerConfig.isComputeInnerJoinCostAsFilter()) {
          joinCost *= CostEstimates.MULTIPLIER_FILTER; // treating inner join like a filter on left table
        }
      }

      if (computeRightRequiresSubquery(getPlannerContext(), getSomeDruidChild(right))) {
        joinCost += CostEstimates.COST_SUBQUERY;
      }
    }

    // Penalize cross joins.
    if (joinRel.getCondition().isA(SqlKind.LITERAL) && !joinRel.getCondition().isAlwaysFalse()) {
      joinCost += CostEstimates.COST_JOIN_CROSS;
    }

    return planner.getCostFactory().makeCost(joinCost, 0, 0);
  }

  public static JoinType toDruidJoinType(JoinRelType calciteJoinType)
  {
    switch (calciteJoinType) {
      case LEFT:
        return JoinType.LEFT;
      case RIGHT:
        return JoinType.RIGHT;
      case FULL:
        return JoinType.FULL;
      case INNER:
        return JoinType.INNER;
      default:
        throw InvalidSqlInput.exception(
            "Cannot handle joinType [%s]",
            calciteJoinType
        );
    }
  }

  public static boolean computeLeftRequiresSubquery(final PlannerContext plannerContext, final DruidRel<?> left, final Join joinRel)
  {
    if (QueryUtils.getJoinAlgorithm(joinRel, plannerContext).requiresSubquery()) {
      return true;
    }

    // Left requires a subquery unless it's a scan or mapping on top of any table or a join.
    return !DruidRels.isScanOrMapping(left, true);
  }

  public static boolean computeRightRequiresSubquery(final PlannerContext plannerContext, final DruidRel<?> right)
  {
    if (plannerContext.getJoinAlgorithm().requiresSubquery()) {
      return true;
    }

    // Right requires a subquery unless it's a scan or mapping on top of a global datasource.
    // ideally this would involve JoinableFactory.isDirectlyJoinable to check that the global datasources
    // are in fact possibly joinable, but for now isGlobal is coupled to joinability
    return !(DruidRels.isScanOrMapping(right, false)
             && DruidRels.druidTableIfLeafRel(right).filter(table -> table.getDataSource().isGlobal()).isPresent());
  }

  public static Set<String> findExistingJoinPrefixes(DataSource... dataSources)
  {
    final ArrayList<DataSource> copy = new ArrayList<>(Arrays.asList(dataSources));

    Set<String> prefixes = new HashSet<>();
    while (!copy.isEmpty()) {
      DataSource current = copy.remove(0);
      copy.addAll(current.getChildren());
      if (current instanceof JoinDataSource) {
        JoinDataSource joiner = (JoinDataSource) current;
        prefixes.add(joiner.getRightPrefix());
      }
    }
    return prefixes;
  }
  /**
   * Returns a Pair of "rightPrefix" (for JoinDataSource) and the signature of rows that will result from
   * applying that prefix.
   */
  public static Pair<String, RowSignature> computeJoinRowSignature(
      final RowSignature leftSignature,
      final RowSignature rightSignature,
      final Set<String> prefixes
  )
  {
    final RowSignature.Builder signatureBuilder = RowSignature.builder();

    for (final String column : leftSignature.getColumnNames()) {
      signatureBuilder.add(column, leftSignature.getColumnType(column).orElse(null));
    }

    StringBuilder base = new StringBuilder("j");
    // the prefixes collection contains all known join prefixes, which might be in use for nested queries but not
    // present in the top level row signatures
    // loop until we are sure we got a new prefix
    String maybePrefix;
    do {
      // Need to include the "0" since findUnusedPrefixForDigits only guarantees safety for digit-initiated suffixes
      maybePrefix = Calcites.findUnusedPrefixForDigits(base.toString(), leftSignature.getColumnNames()) + "0.";
      base.insert(0, "_");
    } while (prefixes.contains(maybePrefix));
    final String rightPrefix = maybePrefix;

    for (final String column : rightSignature.getColumnNames()) {
      signatureBuilder.add(rightPrefix + column, rightSignature.getColumnType(column).orElse(null));
    }

    return Pair.of(rightPrefix, signatureBuilder.build());
  }

  public static DruidRel<?> getSomeDruidChild(final RelNode child)
  {
    if (child instanceof DruidRel) {
      return (DruidRel<?>) child;
    } else {
      final RelSubset subset = (RelSubset) child;
      return (DruidRel<?>) Iterables.getFirst(subset.getRels(), null);
    }
  }

  @Nullable
  private static DimFilter getDimFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final Filter filter
  )
  {
    if (filter == null) {
      return null;
    }
    final RexNode condition = filter.getCondition();
    final DimFilter dimFilter = Expressions.toFilter(
        plannerContext,
        rowSignature,
        null,
        condition
    );
    if (dimFilter == null) {
      throw new CannotBuildQueryException(filter, condition);
    } else {
      return dimFilter;
    }
  }
}
