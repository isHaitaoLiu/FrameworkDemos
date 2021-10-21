package cn.cugcs.sakura.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ACLTest implements Watcher {
    private final Logger logger = LoggerFactory.getLogger(ACLTest.class);
    private ZooKeeper zk = null;
    private final String hostPort;

    @Override
    public void process(WatchedEvent event) {
        logger.info(event.toString());
    }


    public ACLTest(String hostPort) {
        this.hostPort = hostPort;
    }

    void createNode() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
        List<ACL> acls = new ArrayList<>();
        Id auth1 = new Id("digest", DigestAuthenticationProvider.generateDigest("user1:pwd1"));
        Id auth2 = new Id("digest", DigestAuthenticationProvider.generateDigest("user2:pwd2"));
        Id auth3 = new Id("digest", DigestAuthenticationProvider.generateDigest("user3:pwd3"));
        acls.add(new ACL(ZooDefs.Perms.WRITE, auth1));
        acls.add(new ACL(ZooDefs.Perms.CREATE, auth2));
        acls.add(new ACL(ZooDefs.Perms.DELETE, auth3));
        zk.create("/ACLTest", "testNode".getBytes(), acls, CreateMode.PERSISTENT);
    }

    void test1() throws Exception{
        ZooKeeper zk1 = new ZooKeeper(hostPort, 15000, this);
        zk1.addAuthInfo("digest", "user1:pwd1".getBytes());
        try {
            zk1.getData("/ACLTest", false, null);
            logger.info("user1读取成功");
        }catch (KeeperException.NoAuthException e){
            logger.error("user1无读取权限");
        }
        try {
            zk1.setData("/ACLTest", "user1Data".getBytes(), -1);
            logger.info("user1写入成功");
        }catch (KeeperException.NoAuthException e){
            logger.error("user1无写入权限");
        }
    }

    void test2() throws Exception{
        ZooKeeper zk2 = new ZooKeeper(hostPort, 15000, this);
        zk2.addAuthInfo("digest", "user2:pwd2".getBytes());
        try {
            zk2.create("/ACLTest/sub", "subNode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("user2创建成功");
        }catch (KeeperException.NoAuthException e){
            logger.error("user2无创建权限");
        }
        ZooKeeper zk3 = new ZooKeeper(hostPort, 15000, this);
        zk3.addAuthInfo("digest", "user3:pwd3".getBytes());
        try {
            zk3.create("/ACLTest/sub2", "subNode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("user3创建成功");
        }catch (KeeperException.NoAuthException e){
            logger.error("user3无创建权限");
        }
    }

    public static void main(String[] args) throws Exception {
        ACLTest aclTest = new ACLTest(args[0]);
        aclTest.createNode();
        aclTest.test1();
        aclTest.test2();
    }
}
