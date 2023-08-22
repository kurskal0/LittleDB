package gzhu.la.littledb.backend.vm;

import com.google.common.primitives.Bytes;
import gzhu.la.littledb.backend.common.SubArray;
import gzhu.la.littledb.backend.dm.dataitem.DataItem;
import gzhu.la.littledb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [data]
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN+8;
    private static final int OF_DATA = OF_XMAX+8;

    private long uid;
    private VersionManager vm;
    private DataItem dataItem;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid){
        Entry entry = new Entry();
        entry.uid = uid;
        entry.vm = vm;
        entry.dataItem = dataItem;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception{
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data){
        byte[] xMin = Parser.long2Byte(xid);
        byte[] xMax = new byte[8];
        return Bytes.concat(xMin, xMax, data);
    }

    public void release(){
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove(){
        dataItem.release();
    }

    // 以拷贝的形式返回内容
    public byte[] data(){
        dataItem.rLock();
        try {
            SubArray su = dataItem.data();
            byte[] data = new byte[su.end - su.start - OF_DATA];
            System.arraycopy(su.raw, su.start + OF_DATA, data, 0, data.length);
            return data;
        }finally {
            dataItem.rUnlock();
        }
    }

    public long getXMin(){
        dataItem.rLock();
        try {
            SubArray su = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(su.raw, su.start + OF_XMIN, su.start + OF_XMAX));
        }finally {
            dataItem.rUnlock();
        }
    }

    public long getXMax(){
        dataItem.rLock();
        try {
            SubArray su = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(su.raw, su.start + OF_XMAX, su.start + OF_DATA));
        }finally {
            dataItem.rUnlock();
        }
    }

    public void setXMax(long xid) {
        dataItem.before();
        try {
            SubArray su = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, su.raw, su.start + OF_XMAX, 8);
        }finally {
            dataItem.after(xid);
        }
    }

    public long getUid(){
        return uid;
    }
}
