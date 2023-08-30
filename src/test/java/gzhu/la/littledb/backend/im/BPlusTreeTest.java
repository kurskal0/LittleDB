package gzhu.la.littledb.backend.im;

import gzhu.la.littledb.backend.dm.DataManager;
import gzhu.la.littledb.backend.tm.TransactionManager;
import gzhu.la.littledb.backend.tm.TransactionManagerImpl;
import org.junit.Test;

public class BPlusTreeTest {

    @Test
    public void testBPlusTree() throws Exception {
        TransactionManagerImpl tm = TransactionManager.open("D:\\tool\\LittleDB\\im\\im_test");
        DataManager dm = DataManager.open("D:\\tool\\LittleDB\\im\\im_test", 1 << 20, tm);
        //1 生成一个空的根节点，存入到一个dataItem中，会返回一个存放dataItem的rootuid
        //再把rootuid存入，返回存放rootuid的uid
        long uid=BPlusTree.create(dm);
        System.out.println("存放rootuid的是"+uid);
        //2 以该空节点为根节点建立B+树
        //通过uid找到根节点的rootuid（在变），从而定位到根节点
        BPlusTree bt=BPlusTree.load(uid,dm);

        for(int i=1;i<=4;i++){
            System.out.println(i);
            bt.insert(2*i,i*2);
        }
        //当一个Node的子节点满时，生成一个新的邻节点，并重新设置根节点
        //此时的根节点就不是叶子节点了
        for(int i=5;i<=6;i++){
            System.out.println(i);
            bt.insert(2*i,i*3);
        }
        System.out.println(bt.searchRange(8,12));

    }
}
