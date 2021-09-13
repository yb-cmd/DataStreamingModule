package mall.streamer.apps

import com.alibaba.fastjson.{JSON, JSONObject}
import mall.common.constants.MyConstants
import mall.streamer.beans.{Action, ActionsLog, CommonInfo, CouponAlertInfo}
import mall.streamer.utils.MyKafkaUtil

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.elasticsearch.spark._

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks
/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/08/16:31
 * @Description:
 */
object AlertApp extends BaseApp {
  override var appName: String = "AlertApp"
  override var batchDuration: Int = 10

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    //the kafka to elasticsearch connection settings
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes","hadoop102")
    conf.set("es.port","9200")

    context = new StreamingContext(conf, Seconds(batchDuration))

    runApp{

      // the initial data stream
      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.ACTIONS_LOG),context,"0422test")

      var offsetRanges: Array[OffsetRange] = null
      //只有初始的DS(KafkaDS),才能获取Offset
      val ds1: DStream[ActionsLog] = ds.transform(rdd => {

        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 封装为样例类
        rdd.map(record => {

          val jSONObject: JSONObject = JSON.parseObject(record.value())
          // convert the list from java to scala
          val actions: List[Action] = JSON.parseArray(jSONObject.getString("actions"), classOf[Action]).asScala.toList

          //parse common
          val commonInfo: CommonInfo = JSON.parseObject(jSONObject.getString("common"), classOf[CommonInfo])

          ActionsLog(actions, jSONObject.getString("ts").toLong, commonInfo)

        })

      })

      // set a window of 5mins, set (mid，user)as key
      val ds2: DStream[((String, Long), Iterable[ActionsLog])] = ds1.window(Minutes(5))
        .map(log => ((log.common.mid, log.common.uid), log))
        .groupByKey()

      //judge if the user has done actions of change delivery address
      val ds3: DStream[(String, Iterable[ActionsLog])] = ds2.map {
        case ((mid, uid), actionsLogs) => {

          var ifNeedAlert: Boolean = false


          Breaks.breakable {
            //iterate all the actions within that window
            actionsLogs.foreach(actionsLog => {

              actionsLog.actions.foreach(action => {

                if ("trade_add_address".equals(action.action_id)) {

                  ifNeedAlert = true
                  Breaks.break
                }

              })

            })
          }

          if (ifNeedAlert) {
            // if it is needed to be recorded, return the result
            (mid, actionsLogs)
          } else {
            (null, null)
          }

        }

      }

      // filter for those doesn't trigger alerts
      val ds4: DStream[(String, Iterable[ActionsLog])] = ds3.filter(_._1 != null)

      // See the remains if they have two two attempts
      val ds5: DStream[(String, Iterable[Iterable[ActionsLog]])] = ds4.groupByKey()

      val ds6: DStream[(String, Iterable[Iterable[ActionsLog]])] = ds5.filter(_._2.size >= 2)

      // filtered devices
      val ds7: DStream[(String, Iterable[ActionsLog])] = ds6.mapValues(_.flatten)

      // generate alert logs
      val ds8: DStream[CouponAlertInfo] = ds7.map {
        case (mid, actionsLogs) => {

          var uids: mutable.Set[String] = new mutable.HashSet[String]

          var itemIds: mutable.Set[String] = new mutable.HashSet[String]

          var events: ListBuffer[String] = new mutable.ListBuffer[String]

          actionsLogs.foreach(actionsLog => {

            uids.add(actionsLog.common.uid.toString)

            actionsLog.actions.foreach(action => {

              events.append(action.action_id)

              // to return the favored items to see what's arousing suspicious activities
              if ("favor_add".equals(action.action_id)) {
                itemIds.add(action.item)
              }

            })

          })

          val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

          val ts: Long = System.currentTimeMillis()

          val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"))

          val minute: String = dateTime.format(formatter)

          CouponAlertInfo(mid + "_" + minute, uids, itemIds, events, ts)

        }
      }

      ds8.cache()

      println("即将打印：")
      ds8.count().print()

      //insert the data to ElasticSearch
      ds8.foreachRDD(rdd => {


        val index_name= "gmall_coupon_alert" + LocalDate.now()
        /*
            saveToEs(resource: String, cfg: scala.collection.Map[String, String])
            remember to create the index in ES first
            resource: the index to be inserted to
            cfg: settings
         */
        rdd.saveToEs(index_name + "/_doc", Map("es.mapping.id" -> "id"))
        //summit offsets
        ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })


    }

  }
}
