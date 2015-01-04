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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{RuleExecutor, Rule}
import org.apache.spark.sql.sources.{LogicalRelation, BaseRelation}
import org.apache.spark.streaming.dstream.DStream

/** Mix the stream relation creation function into StreamQLConnector. */
trait StreamRelationMixin {
  self: StreamQLConnector =>

  def baseRelationToSchemaDStream(baseRelation: BaseRelation): SchemaDStream = {
    logicalPlanToStreamQuery(LogicalRelation(baseRelation))
  }

  protected lazy val baseRelationConverter = new RuleExecutor[LogicalPlan] {
    lazy val batches: Seq[Batch] =
      Batch("BaseRelation Converter", FixedPoint(100), BaseRelationToLogicalDStream) :: Nil

    object BaseRelationToLogicalDStream extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = plan transform {
        case l @ LogicalRelation(s: StreamTableScan) =>
          LogicalDStream(l.output, s.buildScan())(self)
      }
    }
  }
}

trait StreamRelationProvider {
  def createRelation(streamQLContext: StreamQLConnector, parameter: Map[String, String])
    : StreamBaseRelation
}

abstract class StreamBaseRelation extends BaseRelation {
  val sqlContext = streamQLConnector.qlContext

  def streamQLConnector: StreamQLConnector
}

/**
 * A BaseRelation that can produce all of its tuples as an DStream of Row object.
 */
abstract class StreamTableScan extends StreamBaseRelation {
  def buildScan(): DStream[Row]
}

//TODO. For stream table, partition pruning or filter pushdown is meaningful?
