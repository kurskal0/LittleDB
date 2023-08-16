package gzhu.la.littledb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import gzhu.la.littledb.backend.utils.Error;
import gzhu.la.littledb.backend.utils.Panic;
import gzhu.la.littledb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 *
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 *
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position; // 当前日志指针的位置
    private long fileSize; // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile file, FileChannel fc){
        this.file = file;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile file, FileChannel fc, int xChecksum){
        this.file = file;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init(){
        long size = 0;
        try {
            size = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        if (size < 4){
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        }catch (IOException e){
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    // 检查并移除bad Tail
    private void checkAndRemoveTail(){
        rewind(); // position = 4

        int xCheck = 0;
        while (true){
            // 遍历日志
            byte[] log = internNext();
            if (log == null){
                break;
            }
            xCheck = calChecksum(xCheck, log);
        }
        // xCheck多条日志总的校验和
        if (xCheck != xChecksum){
            Panic.panic(Error.BadLogFileException);
        }
        try {
            truncate(position);
        }catch (Exception e){
            Panic.panic(e);
        }
        try {
            file.seek(position);
        }catch (IOException e){
            Panic.panic(e);
        }
        rewind();
    }

    private int calChecksum(int xCheck, byte[] log){
        for (byte b: log){
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**通过 next() 方法，不断地从文件中读取下一条日志，并将日志文件的 Data 解析出来并返回。
    其中 position 是当前日志文件读到的位置偏移：*/
    private byte[] internNext(){
        //[XChecksum] [Log1] [Log2]
        //[Size] [Checksum] [Data]
        if (position + OF_DATA >= fileSize){
            return null;
        }
        // 读取单条日志的size（data段的字节数）
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        }catch (IOException e){
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + size + OF_DATA > fileSize){
            return null;
        }
        // 读取checksum + data
        ByteBuffer buffer = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        // 单条日志的checksum + data
        byte[] log = buffer.array();
        // 一条日志根据存放的数据算出来的校验和
        int checksum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 一条日志的checksum部分存的东西
        int checksum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checksum1 != checksum2) { // 毁坏的日志文件
            return null;
        }
        position += log.length;
        return log;
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buffer = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buffer);
        }catch (IOException e) {
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    private void updateXChecksum(byte[] log){
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(position);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        }catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data){
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) {
                return null;
            }
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        }catch (IOException e) {
            Panic.panic(e);
        }
    }
}
