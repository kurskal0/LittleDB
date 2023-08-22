package gzhu.la.littledb.backend.vm;

import gzhu.la.littledb.backend.dm.DataManager;
import gzhu.la.littledb.backend.tm.TransactionManager;

public class Visibility {

    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry entry){
        long xmax = entry.getXMin();
        if (t.level == 0){
            return false;
        }else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isSnapShot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry entry){
        if (t.level == 0){
            return readCommitted(tm, t, entry);
        }else {
            return repeatableRead(tm, t, entry);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry entry){
        long xid = t.xid;
        long xmin = entry.getXMin();
        long xmax = entry.getXMax();
        if (xmin == xid && xmax == 0){
            return true;
        }
        if (tm.isCommitted(xmin)){
            if (xmax == 0) return true;
            if (xmax != xid){
                if (!tm.isCommitted(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry entry){
        long xid = t.xid;
        long xmin = entry.getXMin();
        long xmax = entry.getXMax();
        if (xmin == xid && xmax == 0) return true;

        if (tm.isCommitted(xmin) && xmin < xid && !t.isSnapShot(xmin)){
            if (xmax == 0) return true;
            if (xmax != xid){
                return !tm.isCommitted(xmax) || xmax > xid || t.isSnapShot(xmax);
            }
        }
        return false;
    }
}
