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

import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.{BooleanType, StructType}
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
    @transient val sqlConnector: StreamSQLConnector,
    @transient val queryExecution: SQLContext#QueryExecution)
  extends DStream[Row](sqlConnector.streamingContext) {

  def this(sqlConnector: StreamSQLConnector, logicalPlan: LogicalPlan) =
    this(sqlConnector, sqlConnector.sqlContext.executePlan(logicalPlan))

  override def dependencies = parentStreams.toList

  override def slideDuration: Duration = parentStreams.head.slideDuration

  override def compute(validTime: Time): Option[RDD[Row]] = {
    // Set the valid batch duration for this rule to get correct RDD in DStream of this batch
    // duration
    DStreamHelper.setValidTime(validTime)
    // Scan the streaming logic plan to convert streaming plan to specific RDD logic plan.
    Some(queryExecution.executedPlan.execute())
  }

  // To guard out some unsupported logical plans.
  @transient private[streaming] val logicalPlan: LogicalPlan = queryExecution.logical match {
    case _: Command | _: InsertIntoTable | _: CreateTableAsSelect[_] | _: WriteToFile =>
      throw new IllegalStateException(s"logical plan $logicalPlan is not supported currently")
    case _ => queryExecution.logical
  }

  @transient private lazy val parentStreams = {
    def traverse(plan: SparkPlan): Seq[DStream[Row]] = plan match {
      case x: StreamPlan => x.stream :: Nil
      case _ => plan.children.flatMap(traverse(_))
    }
    traverse(queryExecution.executedPlan)
  }

  /**
   * Returns the schema of this SchemaDStream (represented by a [[StructType]]]).
   */
  def schema: StructType = queryExecution.analyzed.schema

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
    new SchemaDStream(sqlConnector, Project(aliases, logicalPlan))
  }

  /**
   * Filters output, only returning those rows where `condition` evaluates to true.
   */
  def where(condition: Expression): SchemaDStream = {
    new SchemaDStream(sqlConnector, Filter(condition, logicalPlan))
  }

  /**
   * Performs a relational join on two SchemaDStreams
   */
  def join(otherPlan: SchemaDStream,
           joinType: JoinType = Inner,
           on: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(sqlConnector,
      Join(logicalPlan, otherPlan.logicalPlan, joinType, on))
  }

  /**
   * Performs a relational join between SchemaDStream and SchemaRDD
   */
  def tableJoin(otherPlan: DataFrame,
                joinType: JoinType = Inner,
                on: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(sqlConnector,
      Join(logicalPlan, otherPlan.logicalPlan, joinType, on))
  }

  /**
   * Sorts the results by the given expressions.
   */
  def orderBy(sortExprs: SortOrder*): SchemaDStream =
    new SchemaDStream(sqlConnector, Sort(sortExprs, global = true, logicalPlan))

  /**
   * Limits the results by the given integers.
   */
  def limit(limitNum: Int): SchemaDStream =
    new SchemaDStream(sqlConnector, Limit(Literal(limitNum), logicalPlan))

  /**
   * Performs a grouping followed by an aggregation.
   */
  def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): SchemaDStream = {
    val aliasedExprs = aggregateExprs.map {
      case ne: NamedExpression => ne
      case e => Alias(e, e.toString)()
    }
    new SchemaDStream(sqlConnector, Aggregate(groupingExprs, aliasedExprs, logicalPlan))
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
    new SchemaDStream(sqlConnector, Subquery(alias.name, logicalPlan))

  /**
   * Combines the tuples of two DStreams with the same schema, keeping duplicates.
   */
  def unionAll(otherPlan: SchemaDStream) =
    new SchemaDStream(sqlConnector, Union(logicalPlan, otherPlan.logicalPlan))

  /**
   * Performs a relational except on two SchemaDStreams.
   */
  def except(otherPlan: SchemaDStream): SchemaDStream =
    new SchemaDStream(sqlConnector, Except(logicalPlan, otherPlan.logicalPlan))

  /**
   * Performs a relational intersect on two SchemaDStreams
   */
  def intersect(otherPlan: SchemaDStream): SchemaDStream =
    new SchemaDStream(sqlConnector, Intersect(logicalPlan, otherPlan.logicalPlan))

  /**
   * Filters tuples using a function over the value of the specified column.
   */
  def where[T1](arg1: Symbol)(udf: (T1) => Boolean) =
    new SchemaDStream(
      sqlConnector,
      Filter(ScalaUdf(udf, BooleanType, Seq(UnresolvedAttribute(arg1.name))), logicalPlan))

  /**
   * :: Experimental ::
   * Returns a sampled version of the underlying dataset.
   */
  @Experimental
  def sample(
      withReplacement: Boolean = true,
      fraction: Double,
      seed: Long) =
    new SchemaDStream(sqlConnector, Sample(fraction, withReplacement, seed, logicalPlan))

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
    new SchemaDStream(sqlConnector, Generate(generator, join, outer, alias, logicalPlan))

  /**
   * Register itself as a temporary stream table.
   */
  def registerAsTable(tableName: String): Unit = {
    sqlConnector.registerDStreamAsTable(this, tableName)
  }
}
