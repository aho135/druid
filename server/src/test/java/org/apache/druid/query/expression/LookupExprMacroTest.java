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

package org.apache.druid.query.expression;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.FunctionTest;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LookupExprMacroTest extends InitializedNullHandlingTest
{
  private static final Expr.ObjectBinding BINDINGS = InputBindings.forInputSuppliers(
      ImmutableMap.<String, InputBindings.InputSupplier<?>>builder()
          .put("x", InputBindings.inputSupplier(ExpressionType.STRING, () -> "foo"))
          .build()
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testLookup()
  {
    assertExpr("lookup(x, 'lookyloo')", "xfoo");
  }
  @Test
  public void testLookupMissingValue()
  {
    assertExpr("lookup(y, 'lookyloo', 'N/A')", "N/A");
    assertExpr("lookup(y, 'lookyloo', null)", null);
  }
  @Test
  public void testLookupNotFound()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Lookup [lookylook] not found");
    assertExpr("lookup(x, 'lookylook')", null);
  }

  @Test
  public void testCacheKeyChangesWhenLookupChanges()
  {
    final String expression = "lookup(x, 'lookyloo')";
    final Expr expr = Parser.parse(expression, LookupEnabledTestExprMacroTable.INSTANCE);
    final Expr exprSameLookup = Parser.parse(expression, LookupEnabledTestExprMacroTable.INSTANCE);
    final Expr exprChangedLookup = Parser.parse(
        expression,
        new ExprMacroTable(LookupEnabledTestExprMacroTable.makeTestMacros(ImmutableMap.of("x", "y", "a", "b")))
    );
    // same should have same cache key
    Assert.assertArrayEquals(expr.getCacheKey(), exprSameLookup.getCacheKey());
    // different should not have same key
    final byte[] exprBytes = expr.getCacheKey();
    final byte[] expr2Bytes = exprChangedLookup.getCacheKey();
    if (exprBytes.length == expr2Bytes.length) {
      // only check for equality if lengths are equal
      boolean allEqual = true;
      for (int i = 0; i < exprBytes.length; i++) {
        allEqual = allEqual && (exprBytes[i] == expr2Bytes[i]);
      }
      Assert.assertFalse(allEqual);
    }
  }

  @Test
  public void testCacheKeyChangesWhenLookupChangesSubExpr()
  {
    final String expression = "concat(lookup(x, 'lookyloo'))";
    final Expr expr = Parser.parse(expression, LookupEnabledTestExprMacroTable.INSTANCE);
    final Expr exprSameLookup = Parser.parse(expression, LookupEnabledTestExprMacroTable.INSTANCE);
    final Expr exprChangedLookup = Parser.parse(
        expression,
        new ExprMacroTable(LookupEnabledTestExprMacroTable.makeTestMacros(ImmutableMap.of("x", "y", "a", "b")))
    );
    // same should have same cache key
    Assert.assertArrayEquals(expr.getCacheKey(), exprSameLookup.getCacheKey());
    // different should not have same key
    final byte[] exprBytes = expr.getCacheKey();
    final byte[] expr2Bytes = exprChangedLookup.getCacheKey();
    if (exprBytes.length == expr2Bytes.length) {
      // only check for equality if lengths are equal
      boolean allEqual = true;
      for (int i = 0; i < exprBytes.length; i++) {
        allEqual = allEqual && (exprBytes[i] == expr2Bytes[i]);
      }
      Assert.assertFalse(allEqual);
    }
  }

  private void assertExpr(final String expression, final Object expectedResult)
  {
    FunctionTest.assertExpr(expression, expectedResult, BINDINGS, LookupEnabledTestExprMacroTable.INSTANCE);
  }
}
