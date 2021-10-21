package cn.cugcs.sakura.zookeeper.selectmaster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.AsyncCallback.*;

import java.io.IOException;
import java.util.UUID;

public class Master implements Watcher {
    private ZooKeeper zk;
    private final String hostPort;
    private final String serverId = UUID.randomUUID().toString();
    boolean isLeader = false;

    Master(String hostPort){
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(
                hostPort,
                15000,
                this
        );
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    /*
    void runForMaster() throws InterruptedException{
        while (true){
            try {
                zk.create(
                        "/master",
                        serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL
                );
            }catch (KeeperException.NodeExistsException e){
                isLeader = false;
                break;
            }catch (KeeperException e){
                e.printStackTrace();
            }
            if (checkMaster()) break;
        }
    }

    boolean checkMaster() throws InterruptedException{
        while (true){
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData(
                        "/master",
                        false,
                        stat
                );
                isLeader = new String(data).equals(serverId);
                return true;
            }catch (KeeperException.NoNodeException e){
                return false;
            }catch (KeeperException e){
                e.printStackTrace();
            }
        }
    }

     */

    StringCallback masterCreateCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
        }
    };

    void runForMaster(){
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }


    DataCallback masterCheckCallBack = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
            }
        }
    };

    void checkMaster(){
        zk.getData("/master", false, masterCheckCallBack, null);
    }
}
