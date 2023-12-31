package gzhu.la.littledb.backend.vm;

import gzhu.la.littledb.backend.utils.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {

    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;  // UID被某个XID持有
    private Map<Long, List<Long>> wait;  // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;  // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;  // XID正在等待的UID
    private Lock lock;

    public LockTable(){
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception{
        lock.lock();
        try {
            //某个xid已经获得的资源的UID列表，如果在这个列表里面，则不造成死锁，也不需要等待
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            //这里表示有了一个新的uid，则把uid加入到u2x和x2u里面，不死锁，不等待
            //u2x  uid被某个xid占有
            if (!u2x.containsKey(uid)) {  // uid xid
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            //以下就是需要等待的情况
            //多个事务等待一个uid的释放
            waitU.put(xid, uid);  //把需要等待uid的xid添加到等待列表里面
//            System.out.println("waitU" + waitU);
            putIntoList(wait, uid, xid);  //uid list<xid> 正在等待uid的xid列表

            // 造成死锁
            if (hasDeadLock()){
                // 从等待列表里删除
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            // 没有造成死锁，但需要等待新的xid获取锁
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        }finally {
            lock.unlock();
        }
    }

    public void remove(long xid){
        lock.lock();
        try {
            List<Long> list = x2u.get(xid);
            if (list != null) {
                while (list.size() > 0) {
                    Long uid = list.remove(0);
                    selectNewXid(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        }finally {
            lock.unlock();
        }
    }

    private void selectNewXid(long uid){
        u2x.remove(uid);  //uid被某个xid持有
        List<Long> list = wait.get(uid);  //正在等待uid的xid的列表
        if (list == null) return;
        assert list.size() > 0;

        while (list.size() > 0) {
            long xid = list.remove(0);  //从等待uid的xid的列表中选一个xid出来
            if (!waitLock.containsKey(xid)){
                continue;
            }else {  //若选出的xid获得了锁
                u2x.put(uid, xid);
                Lock l = waitLock.remove(xid);
                l.unlock();
                break;
            }
        }
        if (list.size() == 0) {
            wait.remove(uid);
        }
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    public boolean hasDeadLock(){
        xidStamp = new HashMap<>();
        stamp = 1;
        System.out.println("xid已经持有哪些uid x2u=" + x2u);//xid已经持有哪些uid
        System.out.println("uid正在被哪个xid占用 u2x=" + u2x);//uid正在被哪个xid占用
        for (long xid : x2u.keySet()){  // 已经拿到锁的xid
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0){
                continue;
            }
            stamp++;
            System.out.println("xid"+xid+"的stamp是"+s);
            System.out.println("进入深搜");
            if (dfs(xid)){
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid){
        Integer stp = xidStamp.get(xid);
        System.out.println("xid" + xid + "的stamp" + stp);
        //遍历某个图时，遇到了之前遍历过的节点，说明出现了环。
        if (stp != null && stp == stamp){
            return true;
        }

        if (stp != null && stp < stamp){
            System.out.println("遇到了前一个图，未成环");
            return false;
        }
        xidStamp.put(xid, stamp);  //每个已获得资源的事务一个独特的stamp
        System.out.println("xidStamp找不到该xid，加入后xidStamp变为"+xidStamp);
        Long uid = waitU.get(xid);
        System.out.println("xid"+xid+"正在等待的uid是"+uid);
        if (uid==null){
            System.out.println("未成环，退出深搜");
            return false;//xid没有需要等待的uid,无死锁
        }

        Long x = u2x.get(uid);  // xid需要等待的uid被哪个xid占用了
        System.out.println("xid"+xid+"需要的uid被"+"xid"+x+"占用了");
        System.out.println("=====再次进入深搜"+"xid"+x+"====");
        assert x != null;
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1){
        List<Long> list = listMap.get(uid0);
        if(list == null) return;
        for(Long l : list){
            if (l == uid1){
                list.remove(l);
                break;
            }
        }
        if (list.size() == 0){
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1){
        if (!listMap.containsKey(uid0)){
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1){
        List<Long> list = listMap.get(uid0);
        if (list == null) return false;
        for (long e : list) {
            if (e == uid1) return true;
        }
        return false;
    }
}
