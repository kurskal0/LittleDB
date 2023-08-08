package gzhu.la.littledb.backend.dm.page;

import gzhu.la.littledb.backend.dm.pageCache.PageCache;
import gzhu.la.littledb.backend.dm.pageCache.PageCacheImpl;
import org.junit.Test;

public class PageTest {

    @Test
    public void testPageOne() throws Exception {
        // 初始化给了16页
        PageCacheImpl pc = PageCache.create("D:\\tool\\LittleDB\\dm\\page", 1 << 17);
        Page page = pc.getForCache(1);
        PageOne.setVcOpen(page);
        PageOne.setVcClose(page);
        // 校验第一页
        boolean flag = PageOne.checkVc(page);
        System.out.println(flag);
    }

    @Test
    public void testPageX() throws Exception {
        PageCacheImpl pc = PageCache.create("D:\\tool\\LittleDB\\dm\\page", 1 << 17);
        byte[] a = new byte[10];
        int pageX1 = pc.newPage(a);
        System.out.println("写入该数据的页号" + pageX1);

        // 从数据库中拿取一页普通页，继续填写
        Page pgx = pc.getPage(pageX1);
        byte[] b = new byte[128];

        long offset = PageX.insert(pgx, b);
        System.out.println("该页写入时的偏移量" + offset);
        System.out.println("该页写完时的偏移量" + PageX.getFSO(pgx));

        offset = PageX.insert(pgx, b);
        System.out.println("该页写入时的偏移量" + offset);
        System.out.println("该页写完时的偏移量" + PageX.getFSO(pgx));
    }
}
