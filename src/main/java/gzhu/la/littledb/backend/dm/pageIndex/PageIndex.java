package gzhu.la.littledb.backend.dm.pageIndex;

import gzhu.la.littledb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 一页中一个区间有多大
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;
    //一个lists代表1页，被分为40个ArrayList，每个区间装的PageInfo类（PageInfo类存储有页号，剩余空间大小）
    //lists存储的是（一个区间，空余容量还剩一个区间的页面集合）（二个区间，空余容量还剩二个区间的页面集合...）
    private List<PageInfo>[] lists;
    private Lock lock;

    @SuppressWarnings("unchecked")
    public PageIndex(){
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public PageInfo select(int spaceSize){
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD; //选择spaceSize需要多大的区间才放得下
            if (number < INTERVALS_NO){
                number++; // 区间若没有超过最大容量，则向上取整
            }
            while (number <= INTERVALS_NO){
                if (lists[number].size() == 0) {  // 没有这么大区间的页数
                    number++;  // 没有正好这么大区间的页数，就把更大容量的给它
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }
    }

    public void add(int pageNumber, int freeSpace){
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pageNumber, freeSpace));
        }finally {
            lock.unlock();
        }
    }
}
