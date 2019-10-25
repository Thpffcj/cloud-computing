package cn.edu.nju;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

public class ApiReturnUtil {

    static Log log = LogFactory.get(WebSocketServer.class);

    public static ApiReturnObject error(String s) {
        log.error(s);
        return new ApiReturnObject(null);
    }

    public static ApiReturnObject success(String cid) {
        log.info("success:" + cid);
        return new ApiReturnObject(null);
    }
}
