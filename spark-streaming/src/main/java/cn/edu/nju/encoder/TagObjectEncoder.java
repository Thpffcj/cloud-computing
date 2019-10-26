package cn.edu.nju.encoder;

import cn.edu.nju.api.TagReturnObject;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.serializer.SimplePropertyPreFilter;

import javax.websocket.EncodeException;
import javax.websocket.Encoder;
import javax.websocket.EndpointConfig;

/**
 * Created by thpffcj on 2019/10/26.
 */
public class TagObjectEncoder implements Encoder.Text<TagReturnObject> {

    @Override
    public String encode(TagReturnObject tagReturnObject) throws EncodeException {
        SimplePropertyPreFilter filter = new SimplePropertyPreFilter(
                TagReturnObject.class, "tagObjects");
        return JSON.toJSONString(tagReturnObject,filter, SerializerFeature.DisableCircularReferenceDetect);
    }

    @Override
    public void init(EndpointConfig endpointConfig) {

    }

    @Override
    public void destroy() {

    }
}
