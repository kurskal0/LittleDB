package gzhu.la.littledb.backend.dm.logger;

import gzhu.la.littledb.backend.utils.Error;
import gzhu.la.littledb.backend.utils.Panic;
import gzhu.la.littledb.backend.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {

    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

    public static Logger create(String path){
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            if (!f.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile file = null;

        try {
            file = new RandomAccessFile(f, "rw");
            fc = file.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }

        ByteBuffer buffer = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buffer);
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
        return new LoggerImpl(file, fc, 0);
    }

    public static Logger open(String path){
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        if (!f.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile file = null;

        try {
            file = new RandomAccessFile(f, "rw");
            fc = file.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }

        LoggerImpl lg = new LoggerImpl(file, fc);
        lg.init();

        return lg;
    }
}
