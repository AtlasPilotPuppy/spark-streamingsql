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

package spark.streamsql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.streaming.{StreamRelationMixin, StreamDDLParser}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.ql.StreamQLConnector

/**
 * The entry point of stream query engine.
 */
class StreamQLContext(
    streamContext: StreamingContext,
    qlContext: SQLContext)
  extends StreamQLConnector(streamContext, qlContext)
  with StreamRelationMixin {

  private lazy val ddlParser = new StreamDDLParser(this)

  override def analyzePlan(plan: LogicalPlan): LogicalPlan = {
    analyzer(baseRelationConverter(plan))
  }

  /**
   * Execute a command or DDL query and directly get the result. The query will be parsed to
   * stream DDL at first, if failed it will fall back to parser in SQLContext.
   */
  override def command(sqlText: String): String = {
    ddlParser(sqlText).map { plan =>
      new SchemaRDD(qlContext, plan)
    }.getOrElse {
      qlContext.sql(sqlText)
    }.collect().map(_.toString()).mkString("\n")
  }
}

