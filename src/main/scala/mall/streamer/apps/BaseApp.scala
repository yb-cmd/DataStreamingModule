package mall.streamer.apps
import org.apache.spark.streaming.StreamingContext
/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/07/18:21
 * @Description: the model for other functions, to save some writing
 */
abstract class BaseApp {
  var appName:String

  var batchDuration:Int

  var context:StreamingContext = null

  def runApp(op: => Unit) {
    try {
      op

      context.start()

      context.awaitTermination()

    } catch {

      case ex: Exception =>{

        ex.printStackTrace()

      }

    }
  }
}
