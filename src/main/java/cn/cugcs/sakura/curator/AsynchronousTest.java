package cn.cugcs.sakura.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsynchronousTest {
    static ExecutorService executorService = Executors.newFixedThreadPool(5);
    private final Logger logger = LoggerFactory.getLogger(AsynchronousTest.class);
    private CuratorFramework zkc;
    private final String hostPort;
    private final String TEST_PATH = "/curator/asynchronous";

    public AsynchronousTest(String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZK(){
        zkc = CuratorFrameworkFactory.newClient(hostPort, new RetryNTimes(10, 3000));
        zkc.start();
    }

    public void test() throws Exception{
        zkc.create().creatingParentContainersIfNeeded()
                .inBackground(callback, TEST_PATH + "/node1" ,executorService)
                .forPath(TEST_PATH + "/node1", "value1".getBytes());
        zkc.create().creatingParentContainersIfNeeded()
                .inBackground(callback, TEST_PATH + "/node2")
                .forPath(TEST_PATH + "/node2", "value2".getBytes());
    }

    BackgroundCallback callback = new BackgroundCallback() {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
            String threadName = Thread.currentThread().getName();
            logger.info("{} is processing", threadName);
            switch (KeeperException.Code.get(event.getResultCode())){
                case OK:
                    String path = (String) event.getContext();
                    String value = new String(client.getData().forPath(path));
                    logger.info("create node success, node value is {}", value);
                    break;
                case NODEEXISTS:
                    logger.error("node is exist");
                    break;
                case CONNECTIONLOSS:
                    logger.warn("connection loss");
                    break;
            }
        }
    };

    public static void main(String[] args) throws Exception{
        AsynchronousTest asynchronousTest = new AsynchronousTest(args[0]);
        asynchronousTest.startZK();
        asynchronousTest.test();
        TimeUnit.SECONDS.sleep(10);
    }
}
