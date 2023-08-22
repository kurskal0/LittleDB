package gzhu.la.littledb.backend.vm;

import gzhu.la.littledb.backend.tm.TransactionManager;
import gzhu.la.littledb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

// vm对一个事务的抽象，以保存快照数据（该事务创建时还活跃这的事务）
public class Transaction {

    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    // 事务id 隔离级别 快照

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active){
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if (level != 0){
            t.snapshot = new HashMap<>();
            for (Long x: active.keySet()){
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isSnapShot(long xid){
        if (xid == TransactionManagerImpl.SUPER_XID){
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
