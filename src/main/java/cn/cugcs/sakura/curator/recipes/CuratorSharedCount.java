package cn.cugcs.sakura.curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class CuratorSharedCount implements SharedCountListener {
    private final Logger logger = LoggerFactory.getLogger(CuratorSharedCount.class);
    private final String hostPort;
    private final String TEST_PATH = "/curator/counter";
    private final int SLEEP_MS = 100;
    private final int THREAD_NUMS = 20;

    public CuratorSharedCount(String hostPort) {
        this.hostPort = hostPort;
    }

    public void test() throws Exception {
        CuratorFramework zkc = CuratorFrameworkFactory.newClient(hostPort, new RetryForever(SLEEP_MS));
        zkc.start();
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUMS);
        for (int i = 0; i < THREAD_NUMS; i++) {
            CuratorFramework zk = CuratorFrameworkFactory.newClient(hostPort, new RetryForever(SLEEP_MS));
            zk.start();
            SharedCount sharedCount = new SharedCount(zkc, TEST_PATH, 0);
            sharedCount.addListener(this);
            sharedCount.start();
            new Thread(() -> {
                try {
                    for (int j = 0; j < 50; j++) {
                        while (true){
                            VersionedValue<Integer> versionedValue = sharedCount.getVersionedValue();
                            int value = sharedCount.getVersionedValue().getValue() + 1;
                            if (sharedCount.trySetCount(versionedValue, value)) break;
                        }
                    }
                    sharedCount.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        SharedCount count = new SharedCount(zkc, TEST_PATH, 0);
        count.start();
        logger.info("count = {}", count.getCount());
        count.close();
    }

    @Override
    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
        logger.info("new count = {}", newCount);
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState){
            case LOST:
                logger.error("connection expired");
                break;
            case RECONNECTED:
                logger.info("reconnect success");
        }
    }

    @Override
    public boolean doNotProxy() {
        return false;
    }

    public static void main(String[] args) throws Exception {
        CuratorSharedCount curatorSharedCount = new CuratorSharedCount(args[0]);
        curatorSharedCount.test();
    }
}
