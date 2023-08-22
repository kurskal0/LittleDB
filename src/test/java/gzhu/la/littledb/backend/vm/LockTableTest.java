package gzhu.la.littledb.backend.vm;

import org.junit.Test;

public class LockTableTest {

    @Test
    public void testDeadLock() throws Exception {
        LockTable lc = new LockTable();
        lc.add(1L, 3L);
        lc.add(2L, 4L);
        lc.add(3L, 5L);
        lc.add(1L, 4L);
        System.out.println("*********");
        lc.add(2L, 5L);
        System.out.println("*********");
        lc.add(3L, 3L);
        System.out.println(lc.hasDeadLock());
    }
}
