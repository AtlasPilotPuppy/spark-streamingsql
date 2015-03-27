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

import org.apache.spark.rdd.{RDD, EmptyRDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Statistics, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

/** A LogicalPlan wrapper of row based DStream. */
private[streaming]
case class LogicalDStream(output: Seq[Attribute], stream: DStream[Row])
    (val streamSqlContext: StreamSQLContext)
  extends LogicalPlan with MultiInstanceRelation {
  def children = Nil

  def newInstance() =
    LogicalDStream(output.map(_.newInstance()), stream)(streamSqlContext).asInstanceOf[this.type]

  @transient override lazy val statistics = Statistics(
    sizeInBytes = BigInt(streamSqlContext.sqlContext.conf.defaultSizeInBytes)
  )
}

/**
 * A PhysicalPlan wrapper of row based DStream, inject the validTime and generate an effective
 * RDD of current batchDuration.
 */
private[streaming]
case class PhysicalDStream(output: Seq[Attribute], @transient stream: DStream[Row])
    extends SparkPlan with StreamPlan {
  import DStreamHelper._

  def children = Nil

  override def execute() = {
    assert(validTime != null)
    Utils.invoke(classOf[DStream[Row]], stream, "getOrCompute", (classOf[Time], validTime))
      .asInstanceOf[Option[RDD[Row]]]
      .getOrElse(new EmptyRDD[Row](sparkContext))
  }
}
