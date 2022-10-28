package com.xm.gmall.realtime.bean

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
case class PageActionLog(
                          mid :String,
                          user_id:String,
                          province_id:String,
                          channel:String,
                          is_new:String,
                          model:String,
                          operate_system:String,
                          version_code:String,
                          page_id:String ,
                          last_page_id:String,
                          page_item:String,
                          page_item_type:String,
                          during_time:Long,
                          action_id:String,
                          action_item:String,
                          action_item_type:String,
                          actionTs: Long,
                          ts:Long
                        ) {
}