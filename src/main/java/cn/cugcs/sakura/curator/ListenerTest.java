package cn.cugcs.sakura.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class ListenerTest {
    private final Logger logger = LoggerFactory.getLogger(ListenerTest.class);
    private CuratorFramework zkc;
    private final String hostPort;
    private final String TEST_PATH = "/curator/listen";

    public ListenerTest(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK(){
        zkc = CuratorFrameworkFactory.newClient(hostPort, new RetryNTimes(5, 3000));
        zkc.getConnectionStateListenable().addListener(connectionStateListener);
        zkc.getUnhandledErrorListenable().addListener(errorListener);
        zkc.getCuratorListenable().addListener(eventListener);
        zkc.start();
    }

    CuratorListener eventListener = new CuratorListener() {
        @Override
        public void eventReceived(CuratorFramework client, CuratorEvent event) {
            try {
                switch (event.getType()){
                    case CHILDREN:
                        logger.info("getChildren() fun called : {}", Arrays.toString(event.getChildren().toArray()));
                        break;
                    case CREATE:
                        logger.info("create() fun called : {}", event.getData());
                        break;
                    case DELETE:
                        logger.info("delete() fun called, path is : {}", event.getPath());
                        break;
                    case GET_DATA:
                        logger.info("get() fun called, data is {}", event.getPath());
                        break;
                    case WATCHED:
                        logger.info("watched() fun called : {}", event.getWatchedEvent());
                        break;

                }
                client.getCuratorListenable().addListener(eventListener);
            }catch (Exception e){
                logger.error("exception while processing event,", e);
                client.close();
            }
        }
    };

    UnhandledErrorListener errorListener = new UnhandledErrorListener() {
        @Override
        public void unhandledError(String message, Throwable e) {
            logger.error("unrecoverable error:" + message, e);
        }
    };

    ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            switch (newState){
                case CONNECTED:
                    logger.info("connect success");
                    break;
                case LOST:
                    logger.info("connect expired");
                    break;
                case READ_ONLY:
                    logger.info("read only");
                    break;
                case SUSPENDED:
                    logger.info("connect loss");
                    break;
                case RECONNECTED:
                    logger.info("reconnect success");
                    break;
            }
        }
    };

    void test() throws Exception{
        zkc.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(TEST_PATH, "data1".getBytes());
        zkc.create().inBackground().forPath(TEST_PATH + "/node1", "data1".getBytes());
        zkc.getChildren().inBackground().forPath(TEST_PATH);
        zkc.getData().inBackground().forPath(TEST_PATH + "/node1");
    }



    public static void main(String[] args) throws Exception{
        ListenerTest listenerTest = new ListenerTest(args[0]);
        listenerTest.startZK();
        listenerTest.test();
        TimeUnit.SECONDS.sleep(3);
    }
}
