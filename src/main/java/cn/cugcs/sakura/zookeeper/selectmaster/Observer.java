package cn.cugcs.sakura.zookeeper.selectmaster;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Observer implements Watcher {
    private ZooKeeper zk;
    private final String hostPort;
    private final Logger logger = LoggerFactory.getLogger(Observer.class);


    public Observer(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info(event.toString());
    }

    void observe(){
        zk.exists("/master", false, existCallback, null);
    }

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeCreated){
                assert "/master".equals(event.getPath());
                listState();
            }
        }
    };

    private void listState(){
        try {
            for (String worker : zk.getChildren("/workers", false)) {
                byte[] data = zk.getData("/workers/" + worker, false, null);
                String name = new String(data);
                System.out.println(worker + ":" + name);
            }
            for (String task :
                    zk.getChildren("/tasks", false)){
                byte[] data = zk.getData("/tasks/" + task, false, null);
                String name = new String(data);
                System.out.println(task + ":" + name);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.toString());
        }
    }

    StatCallback existCallback = new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    observe();
                    break;
                case OK:
                    listState();
                    break;
            }
        }
    };
}
