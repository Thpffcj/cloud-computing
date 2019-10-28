package cn.edu.nju;

import cn.edu.nju.api.ApiReturnObject;
import cn.edu.nju.api.TagReturnObject;
import cn.edu.nju.utils.DateUtils;

/**
 * Created by thpffcj on 2019/10/24.
 */
public class Test {

    public static void main(String[] args) {

        MySQLProcess mySQLProcess = new MySQLProcess();

        ApiReturnObject apiReturnObject = mySQLProcess.getTimeFieldData(DateUtils.getSteamDates());

        TagReturnObject tagReturnObject = mySQLProcess.getTagData(2);

        System.out.println("hello");
    }
}
