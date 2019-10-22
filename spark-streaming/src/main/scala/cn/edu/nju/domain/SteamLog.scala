package cn.edu.nju.domain

import com.alibaba.fastjson.JSONObject

/**
 * Created by thpffcj on 2019/10/21.
 *
 * @param img_src
 * @param game_detail
 * @param original_price
 * @param review_summary
 * @param price
 * @param date
 * @param name
 * @param page
 * @param href
 */
case class SteamLog(img_src: String, game_detail: JSONObject, original_price: String,
                    review_summary: String, price: String, date: String, name: String,
                    page: Int, href: String)
