package cn.edu.nju;

import java.util.ArrayList;

/**
 * Created by thpffcj on 2019/10/24.
 */
public class Test {

    public static void main(String[] args) {

        BatchProcessTest batchProcess = new BatchProcessTest();

        ApiReturnObject apiReturnObject = batchProcess.getTimeFieldData(1398902400, 1401580800);




        System.out.println("hello");
    }
}
