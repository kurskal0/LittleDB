package gzhu.la.littledb.backend.im;

import gzhu.la.littledb.backend.common.SubArray;
import gzhu.la.littledb.backend.dm.dataitem.DataItem;
import gzhu.la.littledb.backend.tm.TransactionManagerImpl;
import gzhu.la.littledb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf){
        if (isLeaf){
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        }else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw){
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys){
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw){
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    static void setRawSibling(SubArray raw, long sibling){
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw){
        return (long) Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8*2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8*2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth){
        int offset = from.start + NODE_HEADER_SIZE + kth * (8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    static void shiftRawKth(SubArray raw, int kth){
        int begin = raw.start + NODE_HEADER_SIZE + (kth+1) * (8*2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8*2)];
        }
    }


    static byte[] newRootRaw(long left, long right, long key){
        //KeyNumber可以不同，但是每个Node的大小都相同
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        //初始两个子节点为 left 和 right, 初始键值为 key。
        setRawIsLeaf(raw, false);  //该节点不是叶子节点
        setRawNoKeys(raw, 2);  //该节点有2个子节点
        setRawSibling(raw, 0);  //根节点无邻节点
        setRawKthSon(raw, left, 0);  //left为第0个子节点的uid
        setRawKthKey(raw, key, 0);  // key值
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    static byte[] newNilRootRaw(){
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);
        return raw.raw;
    }

    static Node loadNode(BPlusTree bTree, long uid) throws Exception{
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public boolean isLeaf(){
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        }finally {
            dataItem.rUnlock();
        }
    }

    class SearchNextRes{
        long uid;  //如果命中，返回命中节点的uid
        long siblingUid;  //如果没有命中，返回邻节点继续查询
    }

    private boolean insert(long uid, long key){
        // 该节点现有几个子节点
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        System.out.println("no" + noKeys);
        while (kth < noKeys){
            long ik = getRawKthKey(raw, kth);
            System.out.println("ik=" + ik);
            if (ik < key){
                kth++;
            }else {
                break;
            }
        }
        // 应该插入到raw的kth位置
        //要插入节点要插在当前节点的最后位置，且当前节点已经有邻节点，返回插入失败，下一步在邻节点进行插入
        if (kth == noKeys && getRawSibling(raw) != 0) return false;
        //如果找到插入位置为kth，且该节点是叶子节点，在当前节点的kth位置插一个key|son
        if (getRawIfLeaf(raw)){
            //从kth开始所有节点往右移动，新的节点插入到kth位置
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys + 1);
        }else {
            System.out.println("插入操作不是叶子节点");
            //思路如果找到插入位置为kth，但该节点不是叶子节点
            //kth位置的索引移到kth+1上，kth位置放入新插入节点的索引
            //kth+1位置的uid改成新插入节点的uid

            //例如 把8插入到节点上
            //   [7 MAX_VALUE]
            //[1 2] [5 7 9]
            //变成
            //   [7 MAX_VALUE]此时，7存放的是[1 2]的uid，MAX_VALUE存放的是[5 7]的uid
            //[1 2] [5 7] [8 9]
            //private boolean insert(long uid, long key)这里uid是[8 9]的uid，key是8
            //如果按照第一个if
            //[7 8 MAX_VALUE]
            //[1 2] [8 9] [5 7]
            //如果按照else
            //[7 8 MAX_VALUE]
            //[1 2] [5 7] [8 9]
            long kk = getRawKthKey(raw, kth);  // kk=MAX_VALUE
            System.out.println(key);
            setRawKthKey(raw, key, kth);  // key = 8, kth = 1
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    private boolean needSplit(){
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    public void release() {
        dataItem.release();
    }

    //searchNext 在当前节点寻找对应 key 的 UID, 如果找不到, 则返回兄弟节点的 UID
    public SearchNextRes searchNext(long key){
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            //System.out.println("no"+noKeys);
            for (int i = 0; i < noKeys; i++) {
                // 第i个节点的key
                System.out.println("遍历到第几个子节点"+i);
                long ik = getRawKthKey(raw, i);
                System.out.println("该子节点的索引是ik="+ik);
                if (key < ik){
                    // 寻找对应key的UID
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            // 如果找不到，则返回兄弟节点的UID
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        }finally {
            dataItem.rUnlock();
        }
    }

    class LeafSearchRangeRes{
        List<Long> uids;  //如果命中，返回范围内所有的uid
        long siblingUid;  //如果没有命中，返回下一个邻节点
    }

    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey){
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);  //该节点有多少个子节点
            int kth = 0;
            while (kth < noKeys){
                // 第k+1个子节点的key
                long ik = getRawKthKey(raw, kth);
                // 找到了满足范围的第一个kth
                if (ik >= leftKey){
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys){
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey){
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                }else{  // 没有满足范围的索引
                    break;
                }
            }
            long siblingUid = 0;
            //该节点搜索完毕，则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
            if (kth == noKeys){
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        }finally {
            dataItem.rUnlock();
        }
    }

    class InsertAndSplitRes{
        long siblingUid, newSon, newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception{
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if (!success){
                //新插入的节点在当前节点的最后，且当前节点已经有邻节点
                //插入不成功，返回邻节点
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            //在插入新节点后raw节点已满的情况下，无法继续插入，
            // 生成一个邻节点插入到raw和raw的邻节点之间，且邻节点会分担一半的数据
            //返回存储邻节点的uid和开头索引
            System.out.println("needSplit: " + needSplit());
            if (needSplit()){
                try {
                    SplitRes splitRes = split();
                    res.newSon = splitRes.newSon;
                    res.newKey = splitRes.newKey;
                    return res;
                }catch (Exception e){
                    err = e;
                    throw e;
                }
            }else {
                return res;
            }
        }finally {
            if (err == null && success){
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            }else{
                dataItem.unBefore();
            }
        }
    }

    class SplitRes{
        long newSon, newKey;
    }

    private SplitRes split() throws Exception{
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        //从BALANCE_NUMBER（复制后一半的数据）开始把raw复制到nodeRaw里面
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        // 插入nodeRaw的uid
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newKey = getRawKthKey(nodeRaw, 0);
        res.newSon = son;
//        System.out.println(res.newKey);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int keyNumber = getRawNoKeys(raw);
        sb.append("keyNumber: ").append(keyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < keyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
