package spark.streamsql.sources

import kafka.serializer.StringDecoder
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.streaming.StreamPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

trait MessageToRowConverter extends Serializable {
  def toRow(message: String): Row
}

class KafkaSource extends SchemaRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {

    require(parameters.contains("topics") &&
      parameters.contains("groupId") &&
      parameters.contains("zkQuorum") &&
      parameters.contains("messageToRow"))

    val topics = parameters("topics").split(",").map { s =>
      val a = s.split(":")
      (a(0), a(1).toInt)
    }.toMap

    val kafkaParams = parameters.get("kafkaParams").map { t =>
      t.split(",").map { s =>
        val a = s.split(":")
        (a(0), a(1))
      }.toMap
    }

    val messageToRow = {
      try {
        val clz = Class.forName(parameters("messageToRow"))
        clz.newInstance().asInstanceOf[MessageToRowConverter]
      } catch {
        case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
      }
    }

    new KafkaRelation(
      parameters("zkQuorum"),
      parameters("groupId"),
      topics,
      kafkaParams,
      messageToRow,
      schema,
      sqlContext)
  }
}

/**
 * `CREATE [TEMPORARY] TABLE kafkaTable(intField, stringField string...) [IF NOT EXISTS]
 * USING spark.streamsql.sources.KafkaSource
 * OPTIONS (topics "aa:1,bb:1",
 *   groupId "test",
 *   zkQuorum "localhost:2181",
 *   kafkaParams "sss:xxx,sss:xxx",
 *   messageToRow "xx.xx.xxx")`
 */
case class KafkaRelation(
    zkQuorum: String,
    groupId: String,
    topics: Map[String, Int],
    params: Option[Map[String, String]],
    messageToRowConverter: MessageToRowConverter,
    val schema: StructType,
    @transient val sqlContext: SQLContext)
  extends StreamBaseRelation
  with StreamPlan {

  private val kafkaParams = Map(
    "zookeeper.connect" -> zkQuorum,
    "group.id" -> groupId,
    "zookeeper.connection.timeout.ms" -> "10000") ++ params.getOrElse(Map())

  // Currently only support Kafka with String messages
  @transient private val kafkaStream = KafkaUtils.createStream[
    String,
    String,
    StringDecoder,
    StringDecoder
    ](streamSqlConnector.streamingContext, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)

  @transient val stream: DStream[Row] = kafkaStream.map(_._2).map(messageToRowConverter.toRow)
}
