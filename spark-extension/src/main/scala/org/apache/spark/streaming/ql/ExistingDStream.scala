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

package org.apache.spark.streaming.ql

import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.sql.{Strategy, Row}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

/** A LogicalPlan wrapper of row based DStream. */
private[streaming]
case class LogicalDStream(output: Seq[Attribute], stream: DStream[Row])
  extends LogicalPlan with MultiInstanceRelation {
  def children = Nil

  def newInstance() =
    LogicalDStream(output.map(_.newInstance()), stream).asInstanceOf[this.type]
}

/**
 * A PhysicalPlan wrapper of row based DStream, inject the validTime and generate an effective
 * RDD of current batchDuration.
 */
private[streaming]
case class PhysicalDStream(output: Seq[Attribute], @transient stream: DStream[Row])
    extends SparkPlan {
  import PhysicalDStream._

  def children = Nil

  override def execute() = {
    assert(validTime != null)
    stream.getOrCompute(validTime).getOrElse(new EmptyRDD[Row](sparkContext))
  }
}

/** Stream related strategies to map stream specific logical plan to physical plan. */
private[streaming] object StreamStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case LogicalDStream(output, stream) => PhysicalDStream(output, stream) :: Nil
    case _ => Nil
  }
}

private[streaming] object PhysicalDStream {
  //TODO. Make this variable as thread local variable
  var validTime: Time = null

  def setValidTime(time: Time): Unit = {
    if (validTime == null) {
      validTime = time
    } else if (validTime != time) {
      validTime = time
    } else {
    }
  }
}
