package cn.cugcs.sakura.curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CuratorAtomicInteger {
    private final Logger logger = LoggerFactory.getLogger(CuratorAtomicInteger.class);
    private final String hostPort;
    private final String TEST_PATH = "/curator/int";
    private final int RETRY_TIMES = 10;
    private final int SLEEP_MS = 50;
    private final int THREAD_NUMS = 30;
    private int b = 0;

    public CuratorAtomicInteger(String hostPort) {
        this.hostPort = hostPort;
    }

    public void testA() throws Exception {
        CuratorFramework zkc = CuratorFrameworkFactory.newClient(hostPort, new RetryForever(SLEEP_MS));
        zkc.start();
        DistributedAtomicInteger a = new DistributedAtomicInteger(zkc, TEST_PATH, new RetryForever(SLEEP_MS));
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUMS);
        for (int i = 0; i < THREAD_NUMS; i++) {
            new Thread(() -> {
                try {
                    CuratorFramework zk = CuratorFrameworkFactory.newClient(hostPort, new RetryForever(SLEEP_MS));
                    zk.start();
                    PromotedToLock lock = PromotedToLock.builder()
                            .lockPath("/curator/int")
                            .retryPolicy(new RetryNTimes(RETRY_TIMES, SLEEP_MS))
                            .timeout(1, TimeUnit.SECONDS)
                            .build();
                    DistributedAtomicInteger integer = new DistributedAtomicInteger(zk, TEST_PATH, new RetryNTimes(RETRY_TIMES, SLEEP_MS), lock);
                    for (int j = 0; j < 50; j++) {
                        while (true){
                            AtomicValue<Integer> value = integer.increment();
                            if (value.succeeded()) {
                                logger.info("{}", value.postValue());
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        logger.info("a = {}", a.get().postValue());
    }

    public void testB() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUMS);
        for (int i = 0; i < THREAD_NUMS; i++) {
            new Thread(() -> {
                try {
                    for (int j = 0; j < 50; j++) {
                        b++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        logger.info("b = {}", b);
    }

    public void testC() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUMS);
        AtomicInteger c = new AtomicInteger();
        for (int i = 0; i < THREAD_NUMS; i++) {
            new Thread(() -> {
                try {
                    for (int j = 0; j < 50; j++) {
                        c.getAndIncrement();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        logger.info("c = {}", c);
    }


    public static void main(String[] args) throws Exception{
        CuratorAtomicInteger curatorAtomicInteger = new CuratorAtomicInteger(args[0]);
        curatorAtomicInteger.testA();
        curatorAtomicInteger.testB();
        curatorAtomicInteger.testC();
    }
}
