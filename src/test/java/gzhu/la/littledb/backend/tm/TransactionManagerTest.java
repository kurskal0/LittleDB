package gzhu.la.littledb.backend.tm;

import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerTest {

    static Random random = new SecureRandom();

    private int transCnt = 0;
    private int noWorkers = 50;  // 工作的线程数
    private int noWorks = 3000;  // 每个工作线程执行的操作数
    private Lock lock = new ReentrantLock();
    private TransactionManager tm;
    private Map<Long, Byte> transMap;
    private CountDownLatch countDownLatch;

    @Test
    public void testMultiThread(){
        tm = TransactionManager.create("D:/tool/LittleDB/tm/trans_test");
        transMap = new ConcurrentHashMap<>();
        countDownLatch = new CountDownLatch(noWorkers);
        for (int i = 0; i < noWorkers; i++) {
            Runnable runnable = () -> worker();
            new Thread(runnable).run();
        }
        try {
            countDownLatch.await();
        }catch (InterruptedException e) {
            e.printStackTrace();
        }
        new File("D:/tool/LittleDB/tm/trans_test.xid").delete();
    }

    public void worker(){
        boolean inTrans = false;  // 标记当前是否在事务中
        long transXID = 0;  // 当前事务的事务ID
        for (int i = 0; i < noWorks; i++) {
            int op = Math.abs(random.nextInt(6));
            if (op == 0) {
                lock.lock();
                if (inTrans == false){  // 如果当前不在事务中
                    long xid = tm.begin();
                    transMap.put(xid, (byte) 0);  // 将事务ID和状态存入tansMap
                    transCnt++;
                    transXID = xid;
                    inTrans = true;
                }else {
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch (status) {
                        case 1:
                            tm.commit(transXID);
                            break;
                        case 2:
                            tm.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);  // 更新事务状态
                    inTrans = false;
                }
                lock.unlock();
            }else {
                lock.lock();
                if (transCnt > 0) {  // 如果存在已提交或已撤销的事务
                    long xid = (random.nextInt(Integer.MAX_VALUE) % transCnt) + 1;  // 随机选择一个事务ID
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tm.isActive(xid);
                            break;
                        case 1:
                            ok = tm.isCommitted(xid);
                            break;
                        case 2:
                            ok = tm.isAborted(xid);
                            break;
                    }
                    assert(ok);
                }
                lock.unlock();
            }
            countDownLatch.countDown();  // 工作线程执行完毕，计数减一
        }
    }
}
