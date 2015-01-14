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

import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{RuleExecutor, Rule}
import org.apache.spark.streaming.Duration

/**
 * Logical plan of time-based window and window pushdown optimizer.
 */
case class WindowedLogicalPlan(
  window: Duration,
  slide: Option[Duration],
  child: LogicalPlan) extends UnaryNode {
  override def output = child.output
}

object WindowPushdown extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case WindowedLogicalPlan(window, slide, p @ LogicalDStream(attr, stream)) =>
      val windowedStream = slide.map { s =>
        stream.window(window, s)
      }.getOrElse {
        stream.window(window)
      }
      LogicalDStream(attr, windowedStream)(p.qlConnector)
  }
}

object WindowOptimizer extends RuleExecutor[LogicalPlan] {
  val batches = Batch("Window Optimizer", FixedPoint(100), WindowPushdown) :: Nil
}
