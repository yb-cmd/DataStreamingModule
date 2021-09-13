package mall.streamer.beans

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/08/15:26
 * @Description: create alert logs
 */
case class Action(action_id:String,
                  item:String,
                  item_type:String,
                  ts:Long)

// actions中的common部分
case class CommonInfo(
                       ar:String,
                       ba:String,
                       ch:String,
                       is_new:Int,
                       md:String,
                       mid:String,
                       os:String,
                       uid:Long,
                       vc:String
                     )

// actions_log主题中的一行数据
case class ActionsLog(
                       actions: List[Action],
                       ts:Long,
                       common:CommonInfo
                     )

// 预警日志
case class CouponAlertInfo(

                            id:String,
                            // the account that the devices logged into
                            uids:mutable.Set[String],
                            itemIds:mutable.Set[String],
                            // all the actions of this device in 5 mins
                            events:ListBuffer[String],
                            // time stamp
                            ts:Long)
