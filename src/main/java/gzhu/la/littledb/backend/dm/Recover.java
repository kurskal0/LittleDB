package gzhu.la.littledb.backend.dm;

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
}
