package cn.edu.nju;

import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import org.springframework.stereotype.Component;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;


@ServerEndpoint(value = "/websocket/{sid}",encoders = {ApiObjectEncoder.class})
@Component
public class WebSocketServer {

    static Log log= LogFactory.get(WebSocketServer.class);
    //静态变量，用来记录当前在线连接数。应该把它设计成线程安全的。
    private static int onlineCount = 0;
    //concurrent包的线程安全Set，用来存放每个客户端对应的MyWebSocket对象。
    private static CopyOnWriteArraySet<WebSocketServer> webSocketSet = new CopyOnWriteArraySet<WebSocketServer>();

    //与某个客户端的连接会话，需要通过它来给客户端发送数据
    private Session session;

    //接收sid
    private String sid="";
    /**
     * 连接建立成功调用的方法*/
    @OnOpen
    public void onOpen(Session session,@PathParam("sid") String sid) {
        this.session = session;
        webSocketSet.add(this);     //加入set中
        addOnlineCount();           //在线数加1
        log.info("有新窗口开始监听:"+sid+",当前在线人数为" + getOnlineCount());
        this.sid=sid;
        GameObject gameObject1 = new GameObject("edge","just so so",2200,"blue");
        GameObject gameObject2 = new GameObject("fire fox","good",900,"green");
        GameObject gameObject3 = new GameObject("chrome","excellent",3800,"red");
        GameObject gameObject4 = new GameObject("edge","just so so",1500,"blue");
        GameObject gameObject5 = new GameObject("fire fox","good",1900,"green");
        GameObject gameObject6 = new GameObject("chrome","excellent",2800,"red");
        GameObject gameObject7 = new GameObject("edge","just so so",2600,"blue");
        GameObject gameObject8 = new GameObject("fire fox","good",2200,"green");
        GameObject gameObject9 = new GameObject("chrome","excellent",1800,"red");
        ArrayList<GameObject> gameObjects1 = new ArrayList<>();
        ArrayList<GameObject> gameObjects2 = new ArrayList<>();
        ArrayList<GameObject> gameObjects3 = new ArrayList<>();
        gameObjects1.add(gameObject1);
        gameObjects1.add(gameObject2);
        gameObjects1.add(gameObject3);
        gameObjects2.add(gameObject4);
        gameObjects2.add(gameObject5);
        gameObjects2.add(gameObject6);
        gameObjects3.add(gameObject7);
        gameObjects3.add(gameObject8);
        gameObjects3.add(gameObject9);
        TimeFieldObject timeFieldObject1 = new TimeFieldObject("2017",gameObjects1);
        TimeFieldObject timeFieldObject2 = new TimeFieldObject("2018",gameObjects2);
        TimeFieldObject timeFieldObject3 = new TimeFieldObject("2019",gameObjects3);
        ArrayList<TimeFieldObject> timeFieldObjects= new ArrayList<>();
        timeFieldObjects.add(timeFieldObject1);
        timeFieldObjects.add(timeFieldObject2);
        timeFieldObjects.add(timeFieldObject3);
        ApiReturnObject apiReturnObject = new ApiReturnObject(timeFieldObjects);
        try {
            sendData(apiReturnObject);
        } catch (IOException | EncodeException e) {
            log.error("websocket IO异常"+e.getMessage());
        }
    }

    /**
     * 连接关闭调用的方法
     */
    @OnClose
    public void onClose() {
        webSocketSet.remove(this);  //从set中删除
        subOnlineCount();           //在线数减1
        log.info("有一连接关闭！当前在线人数为" + getOnlineCount());
    }

    /**
     * 收到客户端消息后调用的方法
     *
     * @param message 客户端发送过来的消息*/
    @OnMessage
    public void onMessage(String message, Session session) {
        log.info("收到来自窗口"+sid+"的信息:"+message);
        //群发消息
        for (WebSocketServer item : webSocketSet) {
            try {
                item.sendMessage(message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param session
     * @param error
     */
    @OnError
    public void onError(Session session, Throwable error) {
        log.error("发生错误");
        error.printStackTrace();
    }

    /**
     * 实现服务器主动推送
     */
    public void sendMessage(String message) throws IOException {
        this.session.getBasicRemote().sendText(message);
    }

    /**
     * 实现服务器主动推送
     */
    public void sendData(ApiReturnObject data) throws IOException, EncodeException {
        this.session.getBasicRemote().sendObject(data);
    }

    /**
     * 群发自定义消息
     * */
    public static void sendInfo(String message,@PathParam("sid") String sid) throws IOException {
        log.info("推送消息到窗口"+sid+"，推送内容:"+message);
        for (WebSocketServer item : webSocketSet) {
            try {
                //这里可以设定只推送给这个sid的，为null则全部推送
                if(sid==null) {
                    item.sendMessage(message);
                }else if(item.sid.equals(sid)){
                    item.sendMessage(message);
                }
            } catch (IOException e) {
                continue;
            }
        }
    }

    public static synchronized int getOnlineCount() {
        return onlineCount;
    }

    public static synchronized void addOnlineCount() {
        WebSocketServer.onlineCount++;
    }

    public static synchronized void subOnlineCount() {
        WebSocketServer.onlineCount--;
    }
}

