package cn.cugcs.sakura.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class APITest {
    private final Logger logger = LoggerFactory.getLogger(APITest.class);
    private CuratorFramework zkc;
    private final String hostPort;
    private final String TEST_PATH = "/curator/sub";

    public APITest(String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZK(){
        zkc = CuratorFrameworkFactory.newClient(hostPort, new RetryNTimes(10, 3000));
        zkc.start();
    }

    public void testFun() throws Exception{
        //create test
        zkc.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(TEST_PATH + "/node1", "node1Value".getBytes());
        zkc.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(TEST_PATH + "/node2", "node2Value".getBytes());
        //getData test
        byte[] data = zkc.getData().forPath(TEST_PATH + "/node1");
        logger.info("node1 data is {}", new String(data));
        //setData test
        zkc.setData().forPath(TEST_PATH + "/node1", "newValue".getBytes());
        data = zkc.getData().forPath(TEST_PATH + "/node1");
        logger.info("node1 data is {}", new String(data));
        //exists test
        Stat stat1 = zkc.checkExists().forPath(TEST_PATH + "/node1");
        Stat stat3 = zkc.checkExists().forPath(TEST_PATH + "/node3");
        if (stat1 != null){
            logger.info("{}{} exist", TEST_PATH, "/node1");
        }else {
            logger.info("{}{} not exist", TEST_PATH, "/node1");
        }
        if (stat3 != null){
            logger.info("{}{} exist", TEST_PATH, "/node3");
        }else {
            logger.info("{}{} not exist", TEST_PATH, "/node3");
        }
        //getChildren test
        listChildren();
        //delete test
        zkc.delete().guaranteed().forPath(TEST_PATH + "/node1");
        listChildren();
    }

    public void listChildren() throws Exception{
        List<String> strings = zkc.getChildren().forPath(TEST_PATH);
        System.out.println(Arrays.toString(strings.toArray()));
    }

    public static void main(String[] args) throws Exception{
        APITest apiTest = new APITest(args[0]);
        apiTest.startZK();
        apiTest.testFun();
    }
}

/*
        zkc.getCuratorListenable().addListener(curatorListener);
            CuratorListener curatorListener = new CuratorListener() {
        @Override
        public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
            try {
                switch (event.getType()){
                    case CREATE:
                        logger.info("create node success {}", client.getData());
                        break;
                    case SET_DATA:
                        logger.info("set data success {}", client.getData());
                        break;
                    case DELETE:
                        logger.info("delete node success");
                        break;
                    case WATCHED:
                        logger.info("watcher success");
                        break;
                }
            }catch (Exception e){
                logger.error("Exception while process event.", e);
                client.close();
            }
        }
    };

        zkc.getData().usingWatcher(new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                switch (event.getType()){
                    case NodeDataChanged:
                        logger.info("data changed");
                        break;
                    case NodeDeleted:
                        logger.info("node deleted");
                        break;
                }
            }
        }).forPath(TEST_PATH + "/node1");
 */