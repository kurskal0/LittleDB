package gzhu.la.littledb.backend.im;

import gzhu.la.littledb.backend.common.SubArray;
import gzhu.la.littledb.backend.dm.DataManager;
import gzhu.la.littledb.backend.dm.dataitem.DataItem;
import gzhu.la.littledb.backend.tm.TransactionManagerImpl;
import gzhu.la.littledb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {

    DataManager dm;
    DataItem bootDataItem;
    long bootUid;
    Lock bootLock;

    public static long create(DataManager dm) throws Exception{
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception{
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    // 返回根节点的uid
    private long rootUid(){
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        }finally {
            bootLock.unlock();
        }
    }

    // left，原来的根节点Uid；right，新的分裂出来的节点Uid；rightKey，新的分裂出来的节点的第一个索引
    private void updateRootUid(long left, long right, long rightKey) throws Exception{
        bootLock.lock();
        try {
            // 生成一个根节点
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            // 返回要插入的根节点的uid
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            // 替换uid为新的uid
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, diRaw.start + 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        }finally {
            bootLock.unlock();
        }
    }

    //从uid为nodeUid的节点开始寻找索引为key的数据的uid(直到找到叶子节点)
    private long searchLeaf(long nodeUid, long key) throws Exception{
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf){
            // 叶子节点
            return nodeUid;
        }else {
            // 找到索引为key的uid，继续往下搜索，直到搜到叶子节点
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    private long searchNext(long nodeUid, long key) throws Exception{
        while (true){
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if (res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception{
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception{
        long rootUid = rootUid();
        //从uid为rootUid的节点开始寻找索引为leftKey的数据的叶子节点的uid
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while (true){
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if (res.siblingUid == 0){
                break;
            }else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }


    // 从根节点开始查找，插入一个新节点
    public void insert(long key, long uid) throws Exception{
        long rootUid = rootUid();

        // 从rootUid节点（一直在变，但永远是bootItem里的值）开始找到要插入的叶子节点位置插入
        InsertRes res = insert(rootUid, uid, key);

        //原节点已满，分裂出一个新的节点，则生成一个根节点，根节点的key保存原节点和新节点的key|uid。
        if (res.newNode != 0){
            System.out.println("根节点已满，需要生成一个新的根节点");
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes{
        long newNode, newKey;
    }

    //从nodeUid节点开始找到要插入的位置插入
    //如果nodeUid是叶子节点，直接插入
    //如果nodeUid不是叶子节点，在下一层找到叶子节点再插入
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception{
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        InsertRes res = null;
        System.out.println("找到的节点是不是叶子节点: "+isLeaf);
        // 如果nodeUid是叶子节点
        if (isLeaf) {
            //往nodeUid大节点处存key|uid信息
            //如果分裂，返回分裂出的节点的信息
            res = insertAndSplit(nodeUid, uid, key);
        }else {
            //（同层查找）在当前节点查找，找不到就返回邻节点查找，返回找到的key的uid或者邻节点的uid
            long next = searchNext(nodeUid, key);
            //如果不是叶子节点，继续在下一层搜索直到找到对应的叶子节点位置，当叶子节点满时，返回新分裂出的节点位置。
            InsertRes insertRes = insert(next, uid, key);
            //把分裂节点插入到一个非叶子节点上
            if (insertRes.newNode != 0){
                //若分裂出节点，把分裂节点的key|uid添加到对应的父节点中。
                //如果原来的父节点已满，就会重新分裂出一个父节点，会返回分裂出的父节点。
                res = insertAndSplit(nodeUid, insertRes.newNode, insertRes.newKey);
            }else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception{
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if (iasr.siblingUid != 0){
                nodeUid = iasr.siblingUid;
            }else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
