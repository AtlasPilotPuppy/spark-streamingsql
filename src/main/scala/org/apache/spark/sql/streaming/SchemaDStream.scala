/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{BooleanType, Row, SchemaRDD, StructType}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

/**
 * :: Experimental ::
 * A row based DStream with schema involved, offer user the ability to manipulate SQL or
 * LINQ-like query on DStream, it is similar to SchemaRDD, which offers the similar function to
 * users. Internally, SchemaDStream treat rdd of each batch duration as a small table, and force
 * query on this small table.
 *
 * The SQL function offered by SchemaDStream is a subset of standard SQL or HiveQL, currently it
 * doesn't support INSERT, CTAS like query in which queried out data need to be written into another
 * destination.
 */
@Experimental
class SchemaDStream(
    val qlConnector: StreamQLConnector,
    val baseLogicalPlan: LogicalPlan)
  extends DStream[Row](qlConnector.streamContext) {

  override def dependencies = physicalStream.toList

  override def slideDuration: Duration = physicalStream.head.slideDuration

  override def compute(validTime: Time): Option[RDD[Row]] = {
    // Set the valid batch duration for this rule to get correct RDD in DStream of this batch
    // duration
    PhysicalDStream.setValidTime(validTime)
    // Scan the streaming logic plan to convert streaming plan to specific RDD logic plan.
    Some(new SchemaRDD(qlConnector.qlContext, preOptimizedPlan))
  }

  // To guard out some unsupported logical plans.
  baseLogicalPlan match {
    case _: Command | _: InsertIntoTable | _: CreateTableAsSelect[_] | _: WriteToFile =>
      throw new IllegalStateException(s"logical plan $baseLogicalPlan is not supported currently")
    case _ => Unit
  }

  private lazy val preOptimizedPlan = qlConnector.preOptimizePlan(baseLogicalPlan)

  private lazy val physicalStream = {
    val tmp = ArrayBuffer[DStream[Row]]()
    preOptimizedPlan.foreach(_ match {
        case LogicalDStream(_, stream) => tmp += stream
        case _ => Unit
      }
    )
    tmp
  }

  /**
   * Returns the schema of this SchemaDStream (represented by a [[StructType]]]).
   */
  lazy val schema: StructType = preOptimizedPlan.schema

  // Streaming DSL query

  /**
   * Changes the output of this relation to the given expressions, similar to the `SELECT` clause
   * in SQL.
   */
  def select(exprs: Expression*): SchemaDStream = {
    val aliases = exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c$i")()
    }
    new SchemaDStream(qlConnector, Project(aliases, baseLogicalPlan))
  }

  /**
   * Filters output, only returning those rows where `condition` evaluates to true.
   */
  def where(condition: Expression): SchemaDStream = {
    new SchemaDStream(qlConnector, Filter(condition, baseLogicalPlan))
  }

  /**
   * Performs a relational join on two SchemaDStreams
   */
  def join(otherPlan: SchemaDStream,
           joinType: JoinType = Inner,
           on: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(qlConnector,
      Join(baseLogicalPlan, otherPlan.baseLogicalPlan, joinType, on))
  }

  /**
   * Performs a relational join between SchemaDStream and SchemaRDD
   */
  def tableJoin(otherPlan: SchemaRDD,
                joinType: JoinType = Inner,
                on: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(qlConnector,
      Join(baseLogicalPlan, otherPlan.logicalPlan, joinType, on))
  }

  /**
   * Sorts the results by the given expressions.
   */
  def orderBy(sortExprs: SortOrder*): SchemaDStream =
    new SchemaDStream(qlConnector, Sort(sortExprs, baseLogicalPlan))

  /**
   * Limits the results by the given integers.
   */
  def limit(limitNum: Int): SchemaDStream =
    new SchemaDStream(qlConnector, Limit(Literal(limitNum), baseLogicalPlan))

  /**
   * Performs a grouping followed by an aggregation.
   */
  def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): SchemaDStream = {
    val aliasedExprs = aggregateExprs.map {
      case ne: NamedExpression => ne
      case e => Alias(e, e.toString)()
    }
    new SchemaDStream(qlConnector, Aggregate(groupingExprs, aliasedExprs, baseLogicalPlan))
  }

  /**
   * Performs an aggregation over all Rows in this DStream. This is equivalent to groupBy with
   * no grouping expressions.
   */
  def aggregate(aggregateExprs: Expression*): SchemaDStream = {
    groupBy()(aggregateExprs: _*)
  }

  /**
   * Applies a qualifier to the attributes of this relation.
   */
  def as(alias: Symbol) =
    new SchemaDStream(qlConnector, Subquery(alias.name, baseLogicalPlan))

  /**
   * Combines the tuples of two DStreams with the same schema, keeping duplicates.
   */
  def unionAll(otherPlan: SchemaDStream) =
    new SchemaDStream(qlConnector, Union(baseLogicalPlan, otherPlan.baseLogicalPlan))

  /**
   * Performs a relational except on two SchemaDStreams.
   */
  def except(otherPlan: SchemaDStream): SchemaDStream =
    new SchemaDStream(qlConnector, Except(baseLogicalPlan, otherPlan.baseLogicalPlan))

  /**
   * Performs a relational intersect on two SchemaDStreams
   */
  def intersect(otherPlan: SchemaDStream): SchemaDStream =
    new SchemaDStream(qlConnector, Intersect(baseLogicalPlan, otherPlan.baseLogicalPlan))

  /**
   * Filters tuples using a function over the value of the specified column.
   */
  def where[T1](arg1: Symbol)(udf: (T1) => Boolean) =
    new SchemaDStream(
      qlConnector,
      Filter(ScalaUdf(udf, BooleanType, Seq(UnresolvedAttribute(arg1.name))), baseLogicalPlan))

  /**
   * :: Experimental ::
   * Filters tuples using a function over a `Dynamic` version of a given Row.  DynamicRows use
   * scala's Dynamic trait to emulate an ORM of in a dynamically typed language.  Since the type of
   * the column is not known at compile time, all attributes are converted to strings before
   * being passed to the function.
   */
  @Experimental
  def where(dynamicUdf: (DynamicRow) => Boolean) =
    new SchemaDStream(
      qlConnector,
      Filter(ScalaUdf(
        dynamicUdf, BooleanType, Seq(WrapDynamic(baseLogicalPlan.output))), baseLogicalPlan))

  /**
   * :: Experimental ::
   * Returns a sampled version of the underlying dataset.
   */
  @Experimental
  def sample(
      withReplacement: Boolean = true,
      fraction: Double,
      seed: Long) =
    new SchemaDStream(qlConnector, Sample(fraction, withReplacement, seed, baseLogicalPlan))

  /**
   * :: Experimental ::
   * Applies the given Generator, or table generating function, to this relation.
   */
  @Experimental
  def generate(
      generator: Generator,
      join: Boolean = false,
      outer: Boolean = false,
      alias: Option[String] = None) =
    new SchemaDStream(qlConnector, Generate(generator, join, outer, alias, baseLogicalPlan))

  /**
   * Register itself as a temporary stream table.
   */
  def registerAsTable(tableName: String): Unit = {
    qlConnector.registerDStreamAsTable(this, tableName)
  }
}
