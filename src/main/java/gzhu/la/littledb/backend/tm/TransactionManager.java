package gzhu.la.littledb.backend.tm;

import gzhu.la.littledb.backend.utils.Error;
import gzhu.la.littledb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {

    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();

    public static TransactionManagerImpl create(String path){
        File file = new File(path + TransactionManagerImpl.XID_FIELD_SUFFIX);
        try{
            if (!file.createNewFile()){
                Panic.panic(Error.FileNotExistsException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }

        ByteBuffer buffer = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path){
        File file = new File(path + TransactionManagerImpl.XID_FIELD_SUFFIX);
        if (!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
