package mall.streamer.apps
import java.time.LocalDate
import java.util

import com.alibaba.fastjson.JSON

import mall.common.constants.MyConstants
import mall.streamer.apps.AlertApp.{appName, batchDuration, context}
import mall.streamer.beans.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import mall.streamer.utils.{MyKafkaUtil, RedisUtil}
import mall.streamer.apps.GMVApp.groupId

import com.google.gson.Gson

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.elasticsearch.spark._

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/08/18:16
 * @Description: for the query part, it has to import the business data
 */
object SaleDetailApp {

  val conf: SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")

  conf.set("es.index.auto.create", "true")
  conf.set("es.nodes","hadoop102")
  conf.set("es.port","9200")

  context = new StreamingContext(conf, Seconds(batchDuration))

  def runApp(unit: Unit) = ???

  runApp{

    val ds1: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.GMALL_ORDER_INFO),context,groupId)
    val ds2: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.GMALL_ORDER_DETAIL),context,groupId)

    var orderInfoOffsetRanges: Array[OffsetRange] = null
    var orderDetailOffsetRanges: Array[OffsetRange] = null

    val ds3: DStream[(String, OrderInfo)] = ds1.transform(rdd => {

      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.map(record => {

        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        (orderInfo.id, orderInfo)

      })

    })

    val ds4: DStream[(String, OrderDetail)] = ds2.transform(rdd => {

      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.map(record => {

        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

        (orderDetail.order_id, orderDetail)

      })

    })

    // Join + buffer
    val ds5: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = ds3.fullOuterJoin(ds4)

    val ds6: DStream[SaleDetail] = ds5.mapPartitions(partition => {

      //connection
      val jedis: Jedis = RedisUtil.getJedisClient()

      val results: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

      partition.foreach {
        case (orderId, (orderInfoOption, orderDetailOption)) => {

          val gson = new Gson()

          if (orderInfoOption != None) {

            val OI: OrderInfo = orderInfoOption.get

            if (orderDetailOption != None) {

              val OD: OrderDetail = orderDetailOption.get

              results.append(new SaleDetail(OI, OD))

            }

            jedis.setex("orderInfo:" + orderId, 5 * 60 * 2, gson.toJson(OI))

            val earlyCommingorderDetails: util.Set[String] = jedis.smembers("orderDetail:" + orderId)

            earlyCommingorderDetails.forEach(orderDetailJsonStr => {

              val detail: OrderDetail = JSON.parseObject(orderDetailJsonStr, classOf[OrderDetail])

              results.append(new SaleDetail(OI, detail))

            })


          } else {

            val OD: OrderDetail = orderDetailOption.get

            val orderInfoJsonStr: String = jedis.get("orderInfo:" + OD.order_id)

            if (orderInfoJsonStr != null) {

              val OI: OrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])

              results.append(new SaleDetail(OI, OD))

            } else {

              jedis.sadd("orderDetail:" + OD.order_id, gson.toJson(OD))

              jedis.expire("orderDetail:" + OD.order_id, 5 * 60 * 2)

            }

          }

        }
      }


      jedis.close()

      results.toIterator

    })

    val ds7: DStream[SaleDetail] = ds6.mapPartitions(partition => {

      val jedis: Jedis = RedisUtil.getJedisClient()

      val result: Iterator[SaleDetail] = partition.map(saleDetail => {

        val userJsonStr: String = jedis.get("user:" + saleDetail.user_id)

        saleDetail.mergeUserInfo(JSON.parseObject(userJsonStr, classOf[UserInfo]))

        saleDetail

      })

      jedis.close()

      result

    })

    ds7.foreachRDD(rdd => {

      val indexName="gmall2021_sale_detail" + LocalDate.now()

      println("即将写入:"+rdd.count())

      rdd.saveToEs(indexName + "/_doc", Map("es.mapping.id" -> "order_detail_id"))

      ds1.asInstanceOf[CanCommitOffsets].commitAsync(orderInfoOffsetRanges)
      ds2.asInstanceOf[CanCommitOffsets].commitAsync(orderDetailOffsetRanges)

    })
  }

}
