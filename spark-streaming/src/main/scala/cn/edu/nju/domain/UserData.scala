package cn.edu.nju.domain

/**
 * Created by thpffcj on 2019/10/19.
 * @param userId 游戏玩家ID号
 * @param gameName 游戏名称
 * @param behavior 玩家购买游戏的行为（购买/玩）
 * @param duration 游戏时长，1代表该买了该游戏
 */
case class UserData(userId:String, gameName:String, behavior:String, duration:Double)
