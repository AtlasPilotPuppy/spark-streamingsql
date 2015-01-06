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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.dsl.ExpressionConversions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.{Row, SQLContext, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * A component to connect StreamingContext with specific ql context ([[SQLContext]] or
 * [[HiveContext]]), offer user the ability to manipulate SQL and LINQ-like query on DStream
 */
class StreamQLConnector(
    val streamContext: StreamingContext,
    val qlContext: SQLContext)
  extends Logging
  with ExpressionConversions
  with UDFRegistrationWrapper {

  // Get several internal fields of SQLContext to better control the flow.
  protected lazy val analyzer = qlContext.analyzer
  protected lazy val catalog = qlContext.catalog
  protected lazy val optimizer = qlContext.optimizer

  // Query parser for streaming specific semantics.
  protected lazy val streamQLParser = new StreamQLParser

  // Add stream specific strategy to the planner.
  qlContext.extraStrategies = StreamStrategy :: Nil

  def preOptimizePlan(plan: LogicalPlan): LogicalPlan = {
    val analyzed = analyzer(plan)
    val optimized = WindowOptimizer(optimizer(analyzed))
    optimized
  }

  /**
   * Create a SchemaDStream from a normal DStream of case classes.
   */
  implicit def createSchemaDStream[A <: Product : TypeTag](stream: DStream[A]): SchemaDStream = {
    SparkPlan.currentContext.set(qlContext)
    val attributes = ScalaReflection.attributesFor[A]
    val schema = StructType.fromAttributes(attributes)
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd(rdd, schema))
    new SchemaDStream(this, LogicalDStream(attributes, rowStream)(this))
  }

  /**
   * :: DeveloperApi ::
   * Allows catalyst LogicalPlans to be executed as a SchemaDStream. Not this logical plan should
   * be streaming meaningful.
   */
  @DeveloperApi
  implicit def logicalPlanToStreamQuery(plan: LogicalPlan): SchemaDStream =
    new SchemaDStream(this, plan)

  /**
   * :: DeveloperApi ::
   * Creates a [[SchemaDStream]] from and [[DStream]] containing [[Row]]s by applying a schema to
   * this DStream.
   */
  @DeveloperApi
  def applySchema(rowStream: DStream[Row], schema: StructType): SchemaDStream = {
    val attributes = schema.toAttributes
    val logicalPlan = LogicalDStream(attributes, rowStream)(this)
    new SchemaDStream(this, logicalPlan)
  }

  /**
   * Register DStream as a temporary table in the catalog. Temporary table exist only during the
   * lifetime of this instance of ql context.
   */
  def registerDStreamAsTable(stream: SchemaDStream, tableName: String): Unit = {
    catalog.registerTable(None, tableName, stream.baseLogicalPlan)
  }

  /**
   * Drop the temporary stream table with given table name in the catalog.
   */
  def dropTable(tableName: String): Unit = {
    catalog.unregisterTable(None, tableName)
  }

  /**
   * Returns the specified stream table as a SchemaDStream
   */
  def table(tableName: String): SchemaDStream = {
    new SchemaDStream(this, catalog.lookupRelation(None, tableName))
  }

  /**
   * Execute a SQL or HiveQL query on stream table, returning the result as a SchemaDStream. The
   * actual parser backed by the initialized ql context.
   */
  def sql(sqlText: String): SchemaDStream = {
    val plan = streamQLParser.parse(sqlText).getOrElse(qlContext.parseSql(sqlText))
    new SchemaDStream(this, plan)
  }

  /**
   * Execute a command or DDL query and directly get the result (depending on the side effect of
   * this command).
   */
  def command(sqlText: String): String = {
    qlContext.sql(sqlText).collect().map(_.toString()).mkString("\n")
  }
}
