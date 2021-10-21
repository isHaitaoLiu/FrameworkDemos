package cn.cugcs.sakura.zookeeper.selectmaster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Worker implements Watcher {
    private final Logger logger = LoggerFactory.getLogger(Worker.class);

    private ZooKeeper zk;
    private final String hostPort;
    private final String serverId = UUID.randomUUID().toString();
    private String status;

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info("{},{}",watchedEvent.toString(), hostPort);
    }

    void register(){
        zk.create("/workers/worker-" + serverId, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

    StringCallback createWorkerCallback = new StringCallback(){
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    logger.info("register successfully:{}", serverId);
                    break;
                case NODEEXISTS:
                    logger.warn("already registered:{}", serverId);
                default:
                    logger.error("error!{}", KeeperException.create(KeeperException.Code.get(rc), path).toString());
            }
        }
    };

    StatCallback statusUpdateCallback = new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
            }
        }
    };


    synchronized private void updateStatus(String status){
        if (this.status.equals(status)){
            zk.setData("/workers/worker-" + serverId, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    public void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }

    public static void main(String[] args) throws Exception{
        Worker worker = new Worker(args[0]);
        worker.startZK();
        worker.register();
        worker.setStatus("working~!");
        TimeUnit.SECONDS.sleep(100);
    }
}
