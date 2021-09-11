package mall.streamer.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/08/14:58
 * @Description: to read the data from kafka and convert to DataStream
 */
object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  val broker_list: String = properties.getProperty("kafka.broker.list")

  def getKafkaStream(topics: Array[String], ssc: StreamingContext, groupId:String, saveOffsetToMysql:Boolean = false ,
                     offsetsMap: Map[TopicPartition, Long] =null , ifAutoCommit:String ="false" ): InputDStream[ConsumerRecord[String, String]] = {

    //kafka consumer settings
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> ifAutoCommit
    )

    var ds: InputDStream[ConsumerRecord[String, String]] =null;

    if (saveOffsetToMysql){

      ds = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam,offsetsMap)
      )
    }else{

      ds = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))
    }

    ds
  }
}
