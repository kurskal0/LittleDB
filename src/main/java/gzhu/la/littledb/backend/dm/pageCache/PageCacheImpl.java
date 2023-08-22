package gzhu.la.littledb.backend.dm.pageCache;

import gzhu.la.littledb.backend.common.AbstractCache;
import gzhu.la.littledb.backend.dm.page.Page;
import gzhu.la.littledb.backend.dm.page.PageImpl;
import gzhu.la.littledb.backend.utils.Error;
import gzhu.la.littledb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    // 记录当前打开的数据库文件有多少页，这个数字在数据库文件打开时就会被计算，并在新建页面时自增。
    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    public int newPage(byte[] initData){
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }

    public Page getPage(int pgno) throws Exception{
        return get((long) pgno);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    //TODO
    @Override
    public Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        // 根据页号算偏移量
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buffer);
        }catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        // this:pageCache的引用，用来方便在拿到page的引用时可以快速对这个页面的缓存进行释放操作
        return new PageImpl(pgno, buffer.array(), this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()){
            flush(pg);
            pg.setDirty(false);
        }
    }


    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        }catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    // 删除指定位置后面的数据页
    @Override
    public void truncateByPgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        }catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    // 页号从1开始
    private static long pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    private void flush(Page pg){
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buffer = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buffer);
            fc.force(false);
        }catch (IOException e) {
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }
}
