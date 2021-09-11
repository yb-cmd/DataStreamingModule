package mall.streamer.apps

import com.alibaba.fastjson.JSON
import mall.common.constants.MyConstants
import mall.streamer.beans.OrderInfo
import mall.streamer.utils.{JDBCUtil, MyJDBCUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/08/15:37
 * @Description: to count the GMV to ensure the "exactly once" data + offsets will be insert into MySQL via one transaction
 */
object GMVApp extends BaseApp {
  override var appName: String = "GMVApp"
  override var batchDuration: Int = 10

  val groupId = "testrr"; // the databases name in MySQL

  def mapRecordToOrderInfo(rdd: RDD[ConsumerRecord[String, String]]):RDD[OrderInfo] = {

    rdd.map(record => {

      val orderInfo: OrderInfo = JSON.parseObject(record.value(),classOf[OrderInfo])

      // "create_time": the mock data app start time  "2021-09-04 16:37:00",
      val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")
      val formatter3: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

      //create_time，parse into LocalDateTime
      val localDateTime: LocalDateTime = LocalDateTime.parse(orderInfo.create_time, formatter3)

      //parse the time in the unified format
      orderInfo.create_date=localDateTime.format(formatter1)
      orderInfo.create_hour=localDateTime.format(formatter2)

      orderInfo

    })

  }

  def main(args: Array[String]): Unit = {

    context= new StreamingContext("local[*]",appName,Seconds(batchDuration))

    //return the offsets of current consumer group
    val offsets: Map[TopicPartition, Long] = MyJDBCUtil.readHitoryOffsetsFromMysql(groupId)

    println(offsets)

    runApp{

      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.GMALL_ORDER_INFO),context,groupId,
        true,offsets)

      ds.foreachRDD(rdd => {

        if (!rdd.isEmpty()) {
          // return the offsets of the current batch
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // ConsumerRecord convert to bean {"payment_way":"2","delivery_address":"YbmfQPyaFanoFkjoRriP","consignee":"ltlWam","create_time":"2021-09-04 16:37:00","order_comment":"CqYUCcEOgpgMryqlfVjl","expire_time":"","order_status":"1","out_trade_no":"5088714924","tracking_no":"","total_amount":"553.0","user_id":"1","img_url":"","province_id":"7","consignee_tel":"13124729998","trade_body":"","id":"2","parent_order_id":"","operate_time":""}
          val rdd1: RDD[OrderInfo] = mapRecordToOrderInfo(rdd)

          //count the batch of data in time frame
          val rdd2: RDD[((String, String), Double)] = rdd1.map(orderinfo => ((orderinfo.create_date, orderinfo.create_hour), orderinfo.total_amount))
            .groupByKey()
            .mapValues(totalAmounts => totalAmounts.sum)

          //collect the data to driver
          val result: Array[((String, String), Double)] = rdd2.collect()

          //to MySQL
          writeDataAndOffsetsToMysql(result,offsetRanges)

        }

      })

    }

  }


  def writeDataAndOffsetsToMysql(result: Array[((String, String), Double)], ranges: Array[OffsetRange]): Unit = {

    var connection : Connection = null
    var ps1: PreparedStatement=null
    var ps2: PreparedStatement=null

    val sql1="INSERT INTO gmvstats(create_date,create_hour,gmv) VALUES(?,?,?) ON DUPLICATE KEY UPDATE gmv = gmv + VALUES(gmv)"

    /* val sql1=
        """
          |
          | INSERT INTO gmvstats(create_date,create_hour,gmv) VALUES(?,?,?)
          |    ON DUPLICATE KEY UPDATE
          |    gmv = gmv + VALUES(gmv)
          |
          |""".stripMargin*/


    val sql2="INSERT INTO offsetsstore(groupid,topic,`partition`,offsets) VALUES(?,?,?,?)  ON DUPLICATE KEY UPDATE  offsets = values(offsets) "
    /*   val sql2=
         """
           |  INSERT INTO offsetsstore(groupid,topic,`partition`,offsets) VALUES(?,?,?,?)
           |    ON DUPLICATE KEY UPDATE
           |    offsets = values(offsets)
           |
           |
           |""".stripMargin*/

    try {
      connection = JDBCUtil.getConnection()

      //cancel the auto summit of transaction
      connection.setAutoCommit(false)

      ps1 = connection.prepareStatement(sql1)
      ps2 = connection.prepareStatement(sql2)

      for (((create_date,create_hour),gmv) <- result) {

        ps1.setString(1,create_date)
        ps1.setString(2,create_hour)
        ps1.setBigDecimal(3,new java.math.BigDecimal(gmv))

        ps1.addBatch()
      }


      val ints: Array[Int] = ps1.executeBatch()

      println("data:"+ints.size)

      for (offsetRange <- ranges) {

        println(ranges.size)

        println("offset"+offsetRange.toString())

        ps2.setString(1,groupId)
        ps2.setString(2,offsetRange.topic)
        ps2.setInt(3,offsetRange.partition)
        // offset of the endpoint
        ps2.setLong(4,offsetRange.untilOffset)

        ps2.addBatch()

      }

      val offsetsSize: Array[Int] = ps2.executeBatch()

      println("offsets"+offsetsSize.size)

      println("------------")

      connection.commit()


    } catch {
      case  e:Exception=> {

        e.printStackTrace()
        println(e.getMessage)

        connection.rollback()

        throw  new RuntimeException("failed!")

      }
    } finally {

      if (ps1 != null){
        ps1.close()
      }

      if (ps2 != null){
        ps1.close()
      }

      if (connection != null){
        connection.close()
      }

    }


  }

}
