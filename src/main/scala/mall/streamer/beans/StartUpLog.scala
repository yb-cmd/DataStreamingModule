package mall.streamer.beans

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/07/18:33
 * @Description:
 */
case class StartUpLog( //common
                       var ar:String,
                       var ba:String,
                       var  ch:String,
                       var is_new:Int,
                       var md:String,
                       var mid:String,
                       var os:String,
                       var  uid:Long,
                       var   vc:String,
                       // start
                       var entry:String,
                       var loading_time:Int,
                       var open_ad_id:Int,
                       var open_ad_ms:Int,
                       var open_ad_skip_ms:Int,
                       var logDate:String,
                       var logHour:String,
                       var ts:Long) {


  def mergeStartInfo(startInfo: StartLogInfo): Unit = {
    // to avoid null value
    if (startInfo != null) {

      this.entry = startInfo.entry
      this.loading_time = startInfo.loading_time
      this.open_ad_id = startInfo.open_ad_id
      this.open_ad_ms = startInfo.open_ad_ms
      this.open_ad_skip_ms = startInfo.open_ad_skip_ms

    }
  }
}
