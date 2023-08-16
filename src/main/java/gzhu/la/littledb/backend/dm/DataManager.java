package gzhu.la.littledb.backend.dm;

import gzhu.la.littledb.backend.dm.dataitem.DataItem;
import gzhu.la.littledb.backend.dm.logger.Logger;
import gzhu.la.littledb.backend.dm.page.PageOne;
import gzhu.la.littledb.backend.dm.pageCache.PageCache;
import gzhu.la.littledb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long uid, byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm){
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 初始化PageOne
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm){
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 校验PageOne
        if (!dm.loadCheckPageOne()){
            Recover.recover(tm, lg, pc);
        }
        //填入每一页的页号和该页的剩余空间
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }

}
