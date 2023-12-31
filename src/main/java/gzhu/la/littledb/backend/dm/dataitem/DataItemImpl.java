package gzhu.la.littledb.backend.dm.dataitem;

import gzhu.la.littledb.backend.common.SubArray;
import gzhu.la.littledb.backend.dm.DataManagerImpl;
import gzhu.la.littledb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem{

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.page = pg;
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    public SubArray data(){
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    public void before(){
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    public void unBefore(){
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    public void after(long xid){
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnlock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
