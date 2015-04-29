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
import org.apache.spark.annotation.{Experimental, DeveloperApi}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.json.JsonRDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * A component to connect StreamingContext with specific ql context ([[SQLContext]] or
 * [[HiveContext]]), offer user the ability to manipulate SQL and LINQ-like query on DStream
 */
class StreamSQLContext(
    val streamingContext: StreamingContext,
    val sqlContext: SQLContext)
  extends Logging {

  // Get internal field of SQLContext to better control the flow.
  protected lazy val catalog = sqlContext.catalog

  // Query parser for streaming specific semantics.
  protected lazy val streamSqlParser = new StreamSQLParser(this)

  // Add stream specific strategy to the planner.
  protected lazy val streamStrategies = new StreamStrategies
  sqlContext.experimental.extraStrategies = streamStrategies.strategies

  /** udf interface for user to register udf through it */
  val udf = sqlContext.udf

  /**
   * Create a SchemaDStream from a normal DStream of case classes.
   */
  implicit def createSchemaDStream[A <: Product : TypeTag](stream: DStream[A]): SchemaDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd(rdd, schema))
    new SchemaDStream(this, LogicalDStream(attributeSeq, rowStream)(this))
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
  def createSchemaDStream(rowStream: DStream[Row], schema: StructType): SchemaDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val attributes = schema.toAttributes
    val logicalPlan = LogicalDStream(attributes, rowStream)(this)
    new SchemaDStream(this, logicalPlan)
  }

  /**
   * Register DStream as a temporary table in the catalog. Temporary table exist only during the
   * lifetime of this instance of sql context.
   */
  def registerDStreamAsTable(stream: SchemaDStream, tableName: String): Unit = {
    catalog.registerTable(Seq(tableName), stream.logicalPlan)
  }

  /**
   * Drop the temporary stream table with given table name in the catalog.
   */
  def dropTable(tableName: String): Unit = {
    catalog.unregisterTable(Seq(tableName))
  }

  /**
   * Returns the specified stream table as a SchemaDStream
   */
  def table(tableName: String): SchemaDStream = {
    new SchemaDStream(this, catalog.lookupRelation(Seq(tableName)))
  }

  /**
   * Execute a SQL or HiveQL query on stream table, returning the result as a SchemaDStream. The
   * actual parser backed by the initialized ql context.
   */
  def sql(sqlText: String): SchemaDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val plan = streamSqlParser(sqlText, false).getOrElse {
      sqlContext.sql(sqlText).queryExecution.logical }
    new SchemaDStream(this, plan)
  }

  /**
   * Execute a command or DDL query and directly get the result (depending on the side effect of
   * this command).
   */
  def command(sqlText: String): String = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    sqlContext.sql(sqlText).collect().map(_.toString()).mkString("\n")
  }

  /**
   * :: Experimental ::
   * Infer the schema from the existing JSON file
   */
  @Experimental
  def inferJsonSchema(path: String, samplingRatio: Double = 1.0): StructType = {
    val colNameOfCorruptedJsonRecord = sqlContext.conf.columnNameOfCorruptRecord
    val jsonRdd = streamingContext.sparkContext.textFile(path)
    JsonRDD.nullTypeToStringType(
      JsonRDD.inferSchema(jsonRdd, samplingRatio, colNameOfCorruptedJsonRecord))
  }

  /**
   * :: Experimental ::
   * Get a SchemaDStream with schema support from a raw DStream of String,
   * in which each string is a json record.
   */
  @Experimental
  def jsonDStream(json: DStream[String], schema: StructType): SchemaDStream = {
    val colNameOfCorruptedJsonRecord = sqlContext.conf.columnNameOfCorruptRecord
    val rowDStream = json.transform { r =>
      JsonRDD.jsonStringToRow(r, schema, colNameOfCorruptedJsonRecord)
    }
    createSchemaDStream(rowDStream, schema)
  }

  /**
   * :: Experimental ::
   * Infer schema from existing json file with `path` and `samplingRatio`. Get the parsed
   * row DStream with schema support from input json string DStream.
   */
  @Experimental
  def jsonDStream(json: DStream[String], path: String, samplingRatio: Double = 1.0)
    : SchemaDStream = {
    jsonDStream(json, inferJsonSchema(path, samplingRatio))
  }
}
