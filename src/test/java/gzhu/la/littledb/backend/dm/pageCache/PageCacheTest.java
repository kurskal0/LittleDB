package gzhu.la.littledb.backend.dm.pageCache;

import gzhu.la.littledb.backend.dm.page.Page;
import org.junit.Test;

public class PageCacheTest {

    @Test
    public void test() throws Exception {
        //        PageCacheImpl pc = PageCache.create("D:\\tool\\LittleDB\\dm\\pageCache", 1 << 20);
        PageCacheImpl pc = PageCache.open("D:\\tool\\LittleDB\\dm\\pageCache", 1 << 20);
        byte[] data = new byte[10];
        int pgno = pc.newPage(data);
        System.out.println(pgno);

        Page page = pc.getPage(2);
        page.setDirty(true);
        page.release();
        page = pc.getForCache(2);
        pc.close();
    }
}
