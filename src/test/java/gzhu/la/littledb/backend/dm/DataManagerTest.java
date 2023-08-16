package gzhu.la.littledb.backend.dm;

import gzhu.la.littledb.backend.dm.logger.Logger;
import gzhu.la.littledb.backend.dm.pageCache.PageCache;
import gzhu.la.littledb.backend.dm.pageCache.PageCacheImpl;
import gzhu.la.littledb.backend.tm.TransactionManager;
import gzhu.la.littledb.backend.tm.TransactionManagerImpl;
import org.junit.Test;

public class DataManagerTest {

    @Test
    public void test() throws Exception {
        TransactionManagerImpl tm = TransactionManager.create("D:\\tool\\LittleDB\\tm\\dm_test");

        long xid = tm.begin();
        DataManager dm = DataManager.create("D:\\tool\\LittleDB\\dm\\dm_test", 1 << 20, tm);

        byte[] b = new byte[1024];
        long uid = dm.insert(2, b);
        System.out.println(uid);
        dm.close();
        tm.commit(xid);
    }

    @Test
    public void testAll(){
        TransactionManagerImpl tm = TransactionManager.open("D:\\tool\\LittleDB\\tm\\dm_test");
        PageCacheImpl pc = PageCache.open("D:\\tool\\LittleDB\\dm\\dm_test", 1 << 17);
        Logger lg = Logger.open("D:\\tool\\LittleDB\\dm\\dm_test");
        Recover.recover(tm, lg, pc);
    }
}
