package cn.cugcs.sakura.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CacheTest {
    static ExecutorService executorService = Executors.newFixedThreadPool(5);
    private final Logger logger = LoggerFactory.getLogger(CacheTest.class);
    private CuratorFramework zkc;
    private final String hostPort;
    private final String TEST_PATH = "/curator/cache";

    public CacheTest(String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZK(){
        zkc = CuratorFrameworkFactory.newClient(hostPort, new RetryNTimes(10, 3000));
        zkc.start();
    }

    public void test() throws Exception{
        zkc.create().creatingParentContainersIfNeeded().forPath(TEST_PATH, "cache".getBytes());
        zkc.create().creatingParentContainersIfNeeded().forPath(TEST_PATH + "/subNode", "subValue".getBytes());
        zkc.setData().forPath(TEST_PATH, "newCache".getBytes());
        zkc.setData().forPath(TEST_PATH + "/subNode", "newSubValue".getBytes());
    }

    public void addNodeCache(){
        CuratorCache curatorCache = CuratorCache.build(zkc, TEST_PATH, CuratorCache.Options.SINGLE_NODE_CACHE);

        NodeCacheListener ncl = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData childData = curatorCache.get(TEST_PATH).get();
                logger.info("ncl : node changed at {}, data is {}", childData.getPath(), new String(childData.getData()));
            }
        };
        CuratorCacheListener curatorCacheListener = CuratorCacheListener
                .builder()
                .forNodeCache(ncl)
                .build();
        curatorCache.listenable().addListener(curatorCacheListener, executorService);
        curatorCache.start();
    }

    void addPathAndTreeCache(){
        CuratorCache curatorCache = CuratorCache.build(zkc, TEST_PATH);

        PathChildrenCacheListener pccl = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                ChildData data = pathChildrenCacheEvent.getData();
                switch (pathChildrenCacheEvent.getType()){
                    case INITIALIZED:
                        logger.info("pccl work!");
                        break;
                    case CHILD_ADDED:
                        logger.info("pccl : node add at {}", data.getPath());
                        break;
                    case CHILD_UPDATED:
                        logger.info("pccl : node changed at {}, data is {}", data.getPath(), new String(data.getData()));
                        break;
                }
            }
        };

        TreeCacheListener tcl = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                switch (event.getType()){
                    case INITIALIZED:
                        logger.info("tcl work!");
                        break;
                    case NODE_ADDED:
                        logger.info("tcl : node add at {}", data.getPath());
                        break;
                    case NODE_UPDATED:
                        logger.info("tcl : node changed at {}, data is {}", data.getPath(), new String(data.getData()));
                        break;
                }
            }
        };
        CuratorCacheListener curatorCacheListener = CuratorCacheListener
                .builder()
                .forPathChildrenCache(TEST_PATH, zkc, pccl)
                .forTreeCache(zkc, tcl)
                .build();
        curatorCache.listenable().addListener(curatorCacheListener, executorService);
        curatorCache.start();
    }


    public static void main(String[] args) throws Exception{
        CacheTest cacheTest = new CacheTest(args[0]);
        cacheTest.startZK();
        cacheTest.addNodeCache();
        cacheTest.addPathAndTreeCache();
        cacheTest.test();
    }
}
