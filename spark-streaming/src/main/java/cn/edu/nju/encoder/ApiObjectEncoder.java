package cn.edu.nju.encoder;

import javax.websocket.EncodeException;
import javax.websocket.Encoder;
import javax.websocket.EndpointConfig;

import cn.edu.nju.api.ApiReturnObject;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.serializer.SimplePropertyPreFilter;

public class ApiObjectEncoder implements Encoder.Text<ApiReturnObject> {

    @Override
    public String encode(ApiReturnObject apiReturnObject) throws EncodeException {
        SimplePropertyPreFilter filter = new SimplePropertyPreFilter(
                ApiReturnObject.class, "timeFieldObjects");
        return JSON.toJSONString(apiReturnObject,filter,SerializerFeature.DisableCircularReferenceDetect);
    }

    @Override
    public void init(EndpointConfig endpointConfig) {

    }

    @Override
    public void destroy() {

    }
}
