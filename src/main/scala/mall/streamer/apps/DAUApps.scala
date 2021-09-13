package mall.streamer.apps

import com.alibaba.fastjson.{JSON, JSONObject}
import mall.common.constants.MyConstants
import mall.streamer.beans.{StartLogInfo, StartUpLog}
import mall.streamer.utils.{MyKafkaUtil, RedisUtil}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/07/18:23
 * @Description: To process and classify the log data
 *               Write the data to Hbase while IDs in Redis, also to process the data to remove any repetition
 */
object DAUApps extends BaseApp {
  override var appName: String = "DAUApp"
  override var batchDuration: Int = 10

  def parseData(rdd:RDD[ConsumerRecord[String, String]]) : RDD[StartUpLog] = {

    rdd.map(record => {

      val jsonobject: JSONObject = JSON.parseObject(record.value())

      // common
      val log: StartUpLog = JSON.parseObject(jsonobject.getString("common"),classOf[StartUpLog])
      // start
      val startlogInfo: StartLogInfo = JSON.parseObject(jsonobject.getString("start"),classOf[StartLogInfo])

      // merge startlogInfo to StartUpLog
      log.mergeStartInfo(startlogInfo)

      // timestamp
      log.ts = jsonobject.getString("ts").toLong

      val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")


      val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(log.ts),ZoneId.of("Asia/Shanghai"))

      log.logDate=dateTime.format(formatter1)
      log.logHour=dateTime.format(formatter2)

      log
    })


  }

  /*
      In order to remove repetition from one batch of data：
              ①it's log data, so dau only counted once a day, classy the data with id
                  ( (mid,logDate) , log)
              ②order ascend and pick the first one
   */
  def removeDuplicateLogInCommonBatch(rdd: RDD[StartUpLog]):RDD[StartUpLog] = {

    val result: RDD[StartUpLog] = rdd.map(log => ((log.mid, log.logDate), log))
      .groupByKey()
      .flatMap {
        case ((mid, logDate), logs) => {
          val logs1: List[StartUpLog] = logs.toList.sortBy(_.ts).take(1)
          logs1
        }
      }
    result
  }

  /*
      to make sure it only get counted once, need to check the mid that was stored in redis
   */
  def removeDuplicateLogInDiffBatch(rdd: RDD[StartUpLog]):RDD[StartUpLog] = {

    rdd.mapPartitions(partition => {

      val jedis: Jedis = RedisUtil.getJedisClient()

      // judge if it was in redis already
      // save the return's true
      val filteredLogs: Iterator[StartUpLog] = partition.filter(log => !jedis.sismember(log.logDate , log.mid))

      jedis.close()

      filteredLogs

    })

  }

  def main(args: Array[String]): Unit = {

    context= new StreamingContext("local[*]",appName,Seconds(batchDuration))

    runApp{

      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.STARTUP_LOG),context,"0422test")

      ds.foreachRDD(rdd => {

        if (!rdd.isEmpty()){
          // the offset of current ds
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // convert consumer record to rdd
          val rdd1: RDD[StartUpLog] = parseData(rdd)

          println("------------------------")
          println("haven't been cleaned:"+rdd1.count())

          // do the cleaning process
          val rdd2: RDD[StartUpLog] = removeDuplicateLogInCommonBatch(rdd1)

          println("after cleaned within the batch:"+rdd2.count())

          //have to remove the repetition between batches：
          val rdd3: RDD[StartUpLog] = removeDuplicateLogInDiffBatch(rdd2)

          rdd3.cache()

          println("Totally Cleaned:"+rdd3.count())

          // write to hbase    RDD ------------> ProductRDDFunctions.saveToPhoenix
          rdd3.saveToPhoenix("MALL2021_STARTUPLOG",
            // 将RDD中的 T类型的每一个属性，写入表的哪些列
            Seq("AR", "BA", "CH", "IS_NEW", "MD", "MID", "OS", "UID", "VC", "ENTRY", "LOADING_TIME","OPEN_AD_ID","OPEN_AD_MS","OPEN_AD_SKIP_MS","LOGDATE","LOGHOUR","TS"),

            // new Configuration 只能读取 hadoop的配置文件，无法读取hbase-site.xml和hbase-default.xml
            HBaseConfiguration.create(),
            Some("hadoop102:2181")
          )

          //write the mid to redis
          rdd3.foreachPartition(partition => {

            val jedis: Jedis = RedisUtil.getJedisClient()

            partition.foreach(log => {

              jedis.sadd(log.logDate , log.mid)
              // how long you want the record to be stored
              jedis.expire(log.logDate, 60 * 60 * 24)

            })

            jedis.close()

          })

          //summit offsets
          ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

        }

      })

    }


  }
}
