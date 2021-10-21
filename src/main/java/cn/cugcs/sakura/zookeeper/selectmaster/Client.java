package cn.cugcs.sakura.zookeeper.selectmaster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Client implements Watcher {
    private final Logger logger = LoggerFactory.getLogger(Client.class);
    private ZooKeeper zk;
    private final String hostPort;

    public Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws Exception{
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info(watchedEvent.toString());
    }

    void queueCommand(String command){
        zk.create("/tasks/task-", command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, taskCreateCallback, command);
    }

    void queueCommandWithTTL(String command, long ttl){
        zk.create("/tasks/task-", command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL, ttlTaskCallback, command, ttl);
    }

    StringCallback taskCreateCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case NODEEXISTS:
                    logger.error("task already exists");
                    return;
                case CONNECTIONLOSS:
                    queueCommand((String) ctx);
                    break;
                case OK:
                    logger.info("task {} created", name);
            }
        }
    };

    Create2Callback ttlTaskCallback = new Create2Callback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case NODEEXISTS:
                    logger.error("task already exists");
                    return;
                case CONNECTIONLOSS:
                    queueCommand((String) ctx);
                    break;
                case OK:
                    logger.info("task {} created", name);
                    break;
                case UNIMPLEMENTED:
                    logger.error("please reset the config");
                    break;
            }
        }
    };

    public static void main(String[] args) throws Exception{
        Client client = new Client(args[0]);
        client.startZK();
        //client.queueCommand("test-task");
        client.queueCommandWithTTL("test", TimeUnit.SECONDS.toMillis(30));
        TimeUnit.SECONDS.sleep(60);
    }
}
