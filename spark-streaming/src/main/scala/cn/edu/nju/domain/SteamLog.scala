package cn.edu.nju.domain

import com.alibaba.fastjson.JSONObject

/**
 * Created by thpffcj on 2019/10/21.
 *
 * @param img_src
 * @param game_detail
 * @param original_price
 * @param price
 * @param review_summary
 * @param date
 * @param name
 */
case class SteamLog(img_src: String, game_detail: String, original_price: String,
                    price: String, review_summary: String, date: String, name: String)
