package gzhu.la.littledb.backend.vm;

import gzhu.la.littledb.backend.dm.DataManager;
import gzhu.la.littledb.backend.tm.TransactionManager;

public interface VersionManager {

    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm){
        return new VersionManagerImpl(tm, dm);
    }
}
