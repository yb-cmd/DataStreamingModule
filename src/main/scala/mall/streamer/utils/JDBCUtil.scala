package mall.streamer.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.kafka.common.TopicPartition
import scala.collection.mutable
/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/07/18:36
 * @Description:
 */
object JDBCUtil {
  private val properties: Properties = PropertiesUtil.load("db.properties")

  var dataSource:DataSource = init()

  def init():DataSource = {

    val paramMap = new java.util.HashMap[String, String]()
    paramMap.put("driverClassName", properties.getProperty("jdbc.driver.name"))
    paramMap.put("url", properties.getProperty("jdbc.url"))
    paramMap.put("username", properties.getProperty("jdbc.user"))
    paramMap.put("password", properties.getProperty("jdbc.password"))
    paramMap.put("maxActive", properties.getProperty("jdbc.datasource.size"))

    DruidDataSourceFactory.createDataSource(paramMap)
  }

  def getConnection(): Connection = {
    dataSource.getConnection
  }

  def main(args: Array[String]): Unit = {

    println(getConnection())

  }

  def readHitoryOffsetsFromMysql(groupId: String) : Map[TopicPartition, Long] = {

    val offsetsMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    val sql:String=
      """
        |
        |SELECT
        |  `topic`,`partition`,`offsets`
        |FROM `offsetsstore`
        |WHERE `groupid`=?
        |
        |
        |""".stripMargin


    try {

      conn=JDBCUtil.getConnection()
      ps=conn.prepareStatement(sql)
      ps.setString(1,groupId)
      rs= ps.executeQuery()

      while(rs.next()){
        val topic: String = rs.getString("topic")
        val partitionid: Int = rs.getInt("partition")
        val offset: Long = rs.getLong("offsets")
        val topicPartition = new TopicPartition(topic, partitionid)
        offsetsMap.put(topicPartition,offset)
      }

    }catch {
      case e:Exception =>
        e.printStackTrace()
        throw new RuntimeException("sth wrong！")

    }finally {

      if (rs != null){
        rs.close()
      }

      if (ps != null){
        ps.close()
      }

      if (conn != null){
        conn.close()
      }
    }

    offsetsMap.toMap
  }
}
