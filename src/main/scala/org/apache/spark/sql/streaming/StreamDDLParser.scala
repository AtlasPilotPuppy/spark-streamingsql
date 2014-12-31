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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.DDLParser
import org.apache.spark.streaming.ql.StreamQLConnector
import org.apache.spark.util.Utils

class StreamDDLParser(
    streamQlConnector: StreamQLConnector with StreamRelationMixin)
  extends DDLParser {

  protected def STREAM = Keyword("STREAM")

  override protected lazy val ddl: Parser[LogicalPlan] = createStreamTable

  protected lazy val createStreamTable: Parser[LogicalPlan] =
    CREATE ~ TEMPORARY ~ STREAM ~ TABLE ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case streamName ~ provider ~ opts =>
        CreateStreamTableUsing(streamName, provider, opts, streamQlConnector)
    }
}

case class CreateStreamTableUsing(
    streamTableName: String,
    provider: String,
    options: Map[String, String],
    streamQlConnector: StreamQLConnector with StreamRelationMixin)
  extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val loader = Utils.getContextOrSparkClassLoader
    val clazz: Class[_] = try loader.loadClass(provider) catch {
      case cnf: ClassNotFoundException =>
        try loader.loadClass(provider + ".DefaultSource") catch {
          case cnf: ClassNotFoundException =>
            sys.error(s"Failed to load class for data source: $provider")
        }
    }

    val dataSource = clazz.newInstance().asInstanceOf[StreamRelationProvider]
    val relation = dataSource.createRelation(streamQlConnector, options)

    val plan = streamQlConnector.baseRelationToSchemaDStream(relation)
    streamQlConnector.registerDStreamAsTable(plan, streamTableName)
    Seq.empty
  }
}
