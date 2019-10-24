package steamserverdemo;

import cn.edu.nju.test.BatchProcessTest;

/**
 * Created by thpffcj on 2019/10/24.
 */
public class Test {

    public static void main(String[] args) {

        BatchProcessTest batchProcess = new BatchProcessTest();
        ApiReturnObject apiReturnObject = batchProcess.getTimeFieldData();

        System.out.println("hello");
    }
}
