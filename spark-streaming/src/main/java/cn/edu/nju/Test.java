package cn.edu.nju;

/**
 * Created by thpffcj on 2019/10/24.
 */
public class Test {

    public static void main(String[] args) {

        MySQLProcess batchProcess = new MySQLProcess();

        ApiReturnObject apiReturnObject = batchProcess.getTimeFieldData(1398902400, 1401580800);




        System.out.println("hello");
    }
}
