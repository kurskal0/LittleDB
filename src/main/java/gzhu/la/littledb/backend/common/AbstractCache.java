package gzhu.la.littledb.backend.common;

import gzhu.la.littledb.backend.utils.Error;
import gzhu.la.littledb.backend.utils.Panic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception{
        //在通过 get() 方法获取资源时，首先进入一个死循环，来无限尝试从缓存里获取。
        // 首先就需要检查这个时候是否有其他线程正在从数据源获取这个资源，如果有，就过会再来看看
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                }catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            //如果资源在缓存中，就可以直接获取并返回了，记得要给资源的引用数 +1。
            // 否则，如果缓存没满的话，就在 getting 中注册一下，该线程准备从数据源获取资源了。
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            if (maxResource > 0 && count == maxResource){
                lock.unlock();
                throw Error.CacheFullException;
            }
            // 如果资源不在缓存中，且缓冲没满，就在 getting 中注册一下，该线程准备从数据源获取资源了。
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        //从数据源获取资源就比较简单了，直接调用那个抽象方法即可，
        // 获取完成记得从 getting 中删除 key。
        T obj = null;
        try {
            //从数据库中获取资源
            obj = getForCache(key);
        }catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //成功从数据库获取资源后，移除getting，把资源放入缓存，引用计数器加1
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        return obj;
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);

    protected void release(long key){
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }else {
                references.put(key, ref);
            }
        }finally {
            lock.unlock();
        }
    }
    
    protected void close(){
        lock.lock();
        try {
            Set<Long> keys = new HashSet<>();
            for (long key : keys){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }
}
