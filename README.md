spark-streamsql
===

**spark-streamsql** is a project based on [Catalyst](https://github.com/apache/spark/tree/master/sql)
and [Spark Streaming](https://github.com/apache/spark/tree/master/streaming), aiming to support
SQL-style queries on data streams. Our target is to advance the progress of Catalyst as well as
Spark Streaming by bridging the gap between structured data queries and stream processing.

Our **spark-streamsql** provides:

1. SQL support on both stream and table data with extended time-based windowing aggregation and join.
2. Easy mutual operation between DStream and SQL.
3. External source API support for streaming source.

### Quick Start ###

#### Creating StreamSQLContext ####

`StreamSQLContext` is the main entry point for all streaming sql related functionalities. `StreamSQLContext` can be created by:

```scala
val ssc: StreamingContext
val sqlContext: SQLContext

val streamSqlContext = new StreamSQLContext(ssc, sqlContext)
```

Or you could use `HiveContext` to get full Hive semantics support, like:

```scala
val ssc: StreamingContext
val hiveContext: HiveContext

val streamSqlContext = new StreamSQLContext(ssc, hiveContext)
```

#### Running SQL on DStreams ####

```scala
case class Person(name: String, age: String)

// Create an DStream of Person objects and register it as a stream.
val people: DStream[Person] = ssc.socketTextStream(serverIP, serverPort)
  .map(_.split(","))
  .map(p => Person(p(0), p(1).toInt))

val schemaPeopleStream = streamSqlContext.createSchemaDStream(people)
schemaPeopleStream.registerAsTable("people")

val teenagers = sql("SELECT name FROM people WHERE age >= 10 && age <= 19")

// The results of SQL queries are themselves DStreams and support all the normal operations
teenagers.map(t => "Name: " + t(0)).print()
ssc.start()
ssc.awaitTerminationOrTimeout(30 * 1000)
ssc.stop()
```

#### Stream Relation Join ####

```scala
val userStream: DStream[User]
streamSqlContext.registerDStreamAsTable(userStream, "user")

val itemStream: DStream[Item]
streamSqlContext.registerDStreamAsTable(itemStream, "item")

sql("SELECT * FROM user JOIN item ON user.id = item.id").print()

val historyItem: DataFrame
historyItem.registerTempTable("history")
sql("SELECT * FROM user JOIN item ON user.id = history.id").print()

```

#### Time Based Windowing Join/Aggregation ####

```scala
sql(
  """
    |SELECT t.word, COUNT(t.word)
    |FROM (SELECT * FROM test) OVER (WINDOW '9' SECONDS, SLIDE '3' SECONDS) AS t
    |GROUP BY t.word
  """.stripMargin)

sql(
  """
    |SELECT * FROM
    |  user1 OVER (WINDOW '9' SECONDS, SLIDE '6' SECONDS) AS u
    |JOIN
    |  user2 OVER (WINDOW '9' SECONDS, SLIDE '6' SECONDS) AS v
    |ON u.id = v.id
    |WHERE u.id > 1 and u.id < 3 and v.id > 1 and v.id < 3
  """.stripMargin)
```

Note: For time-based windowing join, the window size and sliding size should be same for all the
joined streams. This is the limitation of Spark Streaming.

#### External Source API Support for Kafka ####

```scala
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
```

For more examples please checkout the [examples](https://github.com/Intel-bigdata/spark-streamsql/tree/master/src/main/scala/org/apache/spark/sql/streaming/examples).

### How to Build and Deploy ###

**spark-streamsql** is built with sbt, you could use sbt related commands to test/compile/package.

**spark-streamsql** is built on Spark-1.3, you could change the Spark version in `Build.scala`
to the version you wanted, currently **spark-streamsql** can be worked with Spark version 1.3+.

To use **spark-streamsql**, put the packaged jar into your environment where Spark could access,
you could use `spark-submit --jars` or other ways.

---

**FAQs**

Q1. What kind of interfaces are available in the current release version?

The current version only supports Scala DSL programming model. Spark-SQL CLI and JDBC drive are not
supported so far.

Q2. Does it support schema inference from existing Table?

Yes, you could get schema from static source using SparkSQL and apply into streaming clause.

Q3. What kind of SQL standard it follows?

spark-streamsql's SQL coverage relies on SparkSQL, it can support most part of DMLs and some DDLs.

Q4. Can I run chained SQL query in spark-streamsql?

Curretly it does not support such functionalities.

Q5. Does it recognize Hive Metastore ?

Yes, you could initialize StreamSQLContext with HiveContext to get Hive support for spark-streamsql.

Q6. How to run customized functions in spark-streamsql ( to say UDTF, UDAF, UDF ...)?

Yes, you could register UDF through StreamSQLContext.

Q7. Can I insert (overwrite) query results to Table or external store (HBase)?

Not support, you need to handle this through Scala code.

---

This project is open sourced under Apache License Version 2.0.
