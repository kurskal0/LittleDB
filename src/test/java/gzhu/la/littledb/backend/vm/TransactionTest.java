package gzhu.la.littledb.backend.vm;

import gzhu.la.littledb.backend.dm.DataManager;
import gzhu.la.littledb.backend.dm.dataitem.DataItemImpl;
import gzhu.la.littledb.backend.tm.TransactionManager;
import gzhu.la.littledb.backend.tm.TransactionManagerImpl;
import org.junit.Test;

import java.util.Arrays;

public class TransactionTest {

    @Test
    public void readCommittedTest() throws Exception {
        TransactionManagerImpl tm = TransactionManager.create("D:\\tool\\LittleDB\\vm\\vm_test");
        DataManager dm = DataManager.create("D:\\tool\\LittleDB\\vm\\vm_test", 1 << 20, tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        byte[] bytes = new byte[1024];
        long xid1 = vm.begin(0);
        long uid1 = vm.insert(xid1, bytes);
        long xid2 = vm.begin(0);
        byte[] output = vm.read(xid2, uid1);
        System.out.println(Arrays.toString(output));
    }

    @Test
    public void readRepeatedTest() throws Exception {
        TransactionManagerImpl tm = TransactionManager.create("D:\\tool\\LittleDB\\vm\\vm_test");
        DataManager dm = DataManager.create("D:\\tool\\LittleDB\\vm\\vm_test", 1 << 20, tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        byte[] b = new byte[1024];
        long xid1 = vm.begin(0);
        long xid2 = vm.begin(0);
        long uid1 = vm.insert(xid1, b);
        byte[] output = vm.read(xid2, uid1);
        System.out.println(Arrays.toString(output));
        vm.commit(xid1);
        output = vm.read(xid2, uid1);
        System.out.println(Arrays.toString(output));
    }
}
