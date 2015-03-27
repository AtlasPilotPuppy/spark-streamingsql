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

package org.apache.spark.sql.streaming.examples

import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.sql.streaming.sources.MessageToRowConverter
import org.apache.spark.streaming.{Duration, StreamingContext}

class MessageDelimiter extends MessageToRowConverter {
  def toRow(msg: String): Row = Row(msg)
}

object KafkaDDL {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[10]", "test", Duration(3000))
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    streamSqlContext.command(
      """
        |CREATE TEMPORARY TABLE t_kafka (
        |  word string
        |)
        |USING org.apache.spark.sql.streaming.sources.KafkaSource
        |OPTIONS(
        |  zkQuorum "localhost:2181",
        |  groupId  "test",
        |  topics   "aa:1",
        |  messageToRow "org.apache.spark.sql.streaming.examples.MessageDelimiter")
      """.stripMargin)

      streamSqlContext.sql(
      """
        |SELECT t.word, COUNT(t.word)
        |FROM (SELECT * FROM t_kafka) OVER (WINDOW '9' SECONDS, SLIDE '3' SECONDS) AS t
        |GROUP BY t.word
      """.stripMargin)
      .foreachRDD { r => r.foreach(println) }

    ssc.start()
    ssc.awaitTerminationOrTimeout(60 * 1000)
    ssc.stop()
  }

}
