package cn.edu.nju.domain

import com.alibaba.fastjson.JSONObject

/**
 * Created by thpffcj on 2019/10/21.
 * @param support_tags
 * @param user_reviews
 * @param user_tags
 * @param reviewsChart
 */
case class GameDetail(support_tags: Object, user_reviews: JSONObject, user_tags: Object,
                      reviewsChart: JSONObject)
