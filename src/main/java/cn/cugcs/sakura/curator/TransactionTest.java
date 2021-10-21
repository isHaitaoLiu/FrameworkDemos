package cn.cugcs.sakura.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TransactionTest {

    private final Logger logger = LoggerFactory.getLogger(TransactionTest.class);
    private CuratorFramework zkc;
    private final String hostPort;

    public TransactionTest(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws Exception {
        zkc = CuratorFrameworkFactory.newClient(hostPort, new RetryNTimes(5, 3000));
        zkc.start();
        CuratorOp op1 = zkc.transactionOp().delete().forPath("/parent/sub");
        CuratorOp op2 = zkc.transactionOp().delete().forPath("/parent");
        List<CuratorTransactionResult> res = zkc.transaction().forOperations(op1, op2);
    }

    public static void main(String[] args) throws Exception {
        TransactionTest test = new TransactionTest(args[0]);
        test.startZK();
    }
}
