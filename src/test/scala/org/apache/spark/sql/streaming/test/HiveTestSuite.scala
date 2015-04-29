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

import scala.concurrent.duration._

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually

import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}


import scala.collection.mutable.ListBuffer

case class Data(name: String, money: Int)
case class People(name: String, items: Array[String])

class HiveTestSuite extends FunSuite with Eventually with BeforeAndAfter with Logging {

  private var sc: SparkContext = null
  private var ssc: StreamingContext = null
  private var hiveContext: HiveContext = null
  private var streamSQlContext: StreamSQLContext = null

  def beforeFunction()  {
    val conf = new SparkConf().setAppName("streamSQLTest").setMaster("local[4]")
    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Seconds(1))
    hiveContext = new HiveContext(sc)
    streamSQlContext = new StreamSQLContext(ssc, hiveContext)
  }

  def afterFunction()  {
    if (ssc != null) {
      ssc.stop()
    }
  }
  before(beforeFunction)
  after(afterFunction)

  ignore("test udtf")  {
    val dummyRDD = sc.makeRDD(1 to 2).map(i => People("jack"+i,Array("book","gun")))
    val dummyStream = new ConstantInputDStream[People](ssc, dummyRDD)
    val schemaStream = streamSQlContext.createSchemaDStream(dummyStream)
    streamSQlContext.registerDStreamAsTable(schemaStream, "people")
    val resultList = ListBuffer[String]()
    streamSQlContext.sql("select name, item from people lateral view explode(items) items as item ").foreachRDD { rdd =>
      rdd.collect().foreach { row =>
        resultList += row.mkString(",")
      }
    }
    ssc.start()
    val expectedResult = List("jack1,book", "jack1,gun", "jack2,book", "jack2,gun")
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(resultList == expectedResult)
    }
  }


  test("test percentile and stddev UDAF")  {
    val dummyRDD = sc.makeRDD(1 to 10).map(i => Data("jack"+i, i))
    val dummyStream = new ConstantInputDStream[Data](ssc, dummyRDD)
    val schemaStream = streamSQlContext.createSchemaDStream(dummyStream)
    streamSQlContext.registerDStreamAsTable(schemaStream, "data")
    val resultList = ListBuffer[String]()
    streamSQlContext.sql("select percentile(money,0.8), stddev_pop(money) from data  ").foreachRDD { rdd =>
      rdd.collect().foreach { row =>
        resultList += row.toString()
      }
    }
    ssc.start()
    val expectedResult = List("[8.2,2.8722813232690143]")
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(resultList.nonEmpty)
      for (i <- 0 until resultList.size) {
        assert(expectedResult.contains(resultList(i)), "the sql result should be within the expected result set")
      }
    }
  }


}
