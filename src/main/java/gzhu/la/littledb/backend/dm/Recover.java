package gzhu.la.littledb.backend.dm;

import com.google.common.primitives.Bytes;
import gzhu.la.littledb.backend.common.SubArray;
import gzhu.la.littledb.backend.dm.dataitem.DataItem;
import gzhu.la.littledb.backend.dm.logger.Logger;
import gzhu.la.littledb.backend.dm.page.Page;
import gzhu.la.littledb.backend.dm.page.PageX;
import gzhu.la.littledb.backend.dm.pageCache.PageCache;
import gzhu.la.littledb.backend.tm.TransactionManager;
import gzhu.la.littledb.backend.utils.Panic;
import gzhu.la.littledb.backend.utils.Parser;

import java.util.*;

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo{
        long xid;
        int pageNumber;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo{
        long xid;
        int pageNumber;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc){
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;
        while (true){
            byte[] log = lg.next();
            if (log == null) break;
            int pgno;
            if (isInsertLog(log)){
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pageNumber;
            }else {
                UpdateLogInfo xi = parseUpdateLog(log);
                pgno = xi.pageNumber;
            }
            if (pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if (maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc){
        lg.rewind();
        while (true){
            byte[] log = lg.next();  // 返回Logger的数据部分
            if (log == null) break;
            if (isInsertLog(log)){
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (!tm.isActive(xid)){  // 事务不是处于正在被处理的阶段
                    doInsertLog(pc, log, REDO);
                }
            }else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (!tm.isActive(xid)){
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc){
        //事务xid号 list里面装日志
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (tm.isActive(xid)) {  // 未完成的事务，事务对于的日志
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }
        // 对所有active log按照事务xid进行倒序日志undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()){
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)){
                    doInsertLog(pc, log, UNDO);
                }else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log){
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    public static byte[] updateLog(long xid, DataItem dataItem){
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log){
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        // 从 uid 中提取并设置 offset 字段（取低 16 位）
        li.offset = (short) (uid & ((1L << 16) - 1));
        // 将 uid 右移 32 位，获取剩余的高位信息
        uid >>>= 32;
        // 从 uid 中提取并设置 pageNumber 字段（取低 32 位）
        li.pageNumber = (int) (uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length*2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag){
        int pgno;
        short offset;
        byte[] raw;
        if (flag == REDO){
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pageNumber;
            offset = xi.offset;
            raw = xi.newRaw;
        }else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pageNumber;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;

        //从页号在数据库中得到页的对象
        try {
            pg = pc.getPage(pgno);
        }catch (Exception e){
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        }finally {
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw){
        byte[] logTypeRaw = {LOG_TYPE_INSERT}; // 标识日志为插入型
        byte[] xidRaw = Parser.long2Byte(xid); // 事务id
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber()); // 数据库中对应的页号
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg)); // 页的偏移量
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log){
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pageNumber = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag){
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pageNumber);
        }catch (Exception e){
            Panic.panic(e);
        }
        // doInsertLog()方法中的删除，使用的是 DataItem.setDataItemRawInvalid(li.raw);
        // dataItem 将在下一节中说明，大致的作用，就是将该条 DataItem 的有效位设置为无效，来进行逻辑删除
        try {
            if (flag == UNDO){
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        }finally {
            pg.release();
        }
    }
}
