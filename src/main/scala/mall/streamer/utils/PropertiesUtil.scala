package mall.streamer.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/07/18:39
 * @Description: the load method to load the properties
 */
object PropertiesUtil {
  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

  def main(args: Array[String]): Unit = {

    println(load("config.properties"))

  }
}
