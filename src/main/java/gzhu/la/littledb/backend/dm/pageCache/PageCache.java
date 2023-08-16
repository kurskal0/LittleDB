package gzhu.la.littledb.backend.dm.pageCache;

import gzhu.la.littledb.backend.dm.page.Page;
import gzhu.la.littledb.backend.utils.Error;
import gzhu.la.littledb.backend.utils.Panic;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13;  // 2 ^ 13 = 8192

    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory){
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        }catch (Exception e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        }catch (IOException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) (memory / PAGE_SIZE));
    }

    public static PageCacheImpl open(String path, long memory){
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        if (!file.canRead() || !file.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        }catch (IOException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) (memory / PAGE_SIZE));
    }
}
