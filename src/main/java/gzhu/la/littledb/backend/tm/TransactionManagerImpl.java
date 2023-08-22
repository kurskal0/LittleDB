package gzhu.la.littledb.backend.tm;

import gzhu.la.littledb.backend.utils.Error;
import gzhu.la.littledb.backend.utils.Panic;
import gzhu.la.littledb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{

    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;

    // 每个事务文件的长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，永远处于提交状态
    public static final long SUPER_XID = 0;

    static final String XID_FIELD_SUFFIX = ".xid";

    private FileChannel fc;
    private RandomAccessFile file;
    private long xidCounter;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc){
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    private void checkXIDCounter(){
        long fileLength = 0;
        try {
            fileLength = file.length();
        }catch (IOException e1){
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLength < LEN_XID_HEADER_LENGTH){
            //对于校验没有通过的，会直接通过 panic 方法，强制停机。
            // 在一些基础模块中出现错误都会如此处理，
            // 无法恢复的错误只能直接停机。
            Panic.panic(Error.BadXIDFileException);
        }
        // java NIO中的Buffer的array()方法在能够读和写之前，必须有一个缓冲区，
        // 用静态方法 allocate() 来分配缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buffer);
        }catch (IOException e) {
            Panic.panic(e);
        }
        //从文件开头8个字节得到事务的个数
        this.xidCounter = Parser.parseLong(buffer.array());

        // 根据事务xid取得其在xid文件中对应的位置
        long end = getXidPosition(xidCounter + 1);

        if (end != fileLength){
            //对于校验没有通过的，会直接通过 panic 方法，强制停机
            Panic.panic(Error.BadXIDFileException);
        }
    }


    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid){
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    private void incrXIDCounter(){
        xidCounter++;
        ByteBuffer buffer = ByteBuffer.wrap(Parser.long2Byte(xidCounter));

        //游标pos， 限制为lim， 容量为cap
        try {
            fc.position(0);
            fc.write(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        try {
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    private void updateXID(long xid, byte status){
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];  // 每个事务占用长度
        tmp[0] = status;
        ByteBuffer buffer = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buffer);
        }catch (IOException e) {
            Panic.panic(e);
        }
        try {
            //对于校验没有通过的，会直接通过 panic 方法，强制停机
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    public boolean checkXID(long xid, byte status){
        long offset = getXidPosition(xid);
        //ByteBuffer俗称缓冲器， 是将数据移进移出通道的唯一方式
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        return buffer.array()[0] == status;
    }

    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);  // 正在进行xid
            incrXIDCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        }catch (IOException e) {
            Panic.panic(e);
        }
    }
}
