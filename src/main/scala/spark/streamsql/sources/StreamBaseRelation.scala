package spark.streamsql.sources

import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.StreamPlan

abstract class StreamBaseRelation extends BaseRelation with StreamPlan
