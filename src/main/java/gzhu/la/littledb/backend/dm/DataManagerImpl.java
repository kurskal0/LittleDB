package gzhu.la.littledb.backend.dm;

import gzhu.la.littledb.backend.common.AbstractCache;
import gzhu.la.littledb.backend.dm.dataitem.DataItem;
import gzhu.la.littledb.backend.dm.dataitem.DataItemImpl;
import gzhu.la.littledb.backend.dm.logger.Logger;
import gzhu.la.littledb.backend.dm.page.Page;
import gzhu.la.littledb.backend.dm.page.PageOne;
import gzhu.la.littledb.backend.dm.page.PageX;
import gzhu.la.littledb.backend.dm.pageCache.PageCache;
import gzhu.la.littledb.backend.dm.pageIndex.PageIndex;
import gzhu.la.littledb.backend.dm.pageIndex.PageInfo;
import gzhu.la.littledb.backend.tm.TransactionManager;
import gzhu.la.littledb.backend.utils.Error;
import gzhu.la.littledb.backend.utils.Panic;
import gzhu.la.littledb.backend.utils.Types;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger,TransactionManager tm){
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pageIndex = new PageIndex();
    }

    /**
     * 从缓存中获取数据项
     *
     * @param uid 数据项的唯一标识
     * @return 缓存中的数据项
     * @throws Exception 如果获取数据项时发生异常
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        // 根据唯一标识计算偏移量
        short offset = (short) (uid & (1L << 16) - 1);

        // 将唯一标识右移32位，获取页号
        uid >>>= 32;
        int pageNumber = (int) (uid & (1L << 32) - 1);

        // 通过页号获取对应的页面
        Page page = pc.getPage(pageNumber);

        // 解析页面中的数据项，并返回结果
        return DataItem.parseDataItem(page, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()){
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //page里的data是dataItem
        byte[] raw = DataItem.wrapDataItemRaw(data);  // 打包成dataItem的格式
        if (raw.length > PageX.MAX_FREE_SPACE){
            throw Error.DataTooLargeException;
        }

        // 尝试获取可用页
        PageInfo pi = null;
        for (int i = 0; i < 5; i++) {
            pi = pageIndex.select(raw.length);
            if (pi != null){
                break;
            }else{
                //没有满足条件的数据页，新建一个数据页并写入数据库文件
                int newPgno = pc.newPage(PageX.initRaw());
                pageIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi == null){
            throw Error.DatabaseBusyException;
        }

        System.out.println(pi.pageNumber);
        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pageNumber);
            // 首先做日志  raw dataItem page里的data
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            // 再执行插入操作
            short offset = PageX.insert(pg, raw);
            pg.release();
            //返回插入位置的偏移
            return Types.addressToUid(pi.pageNumber, offset);
        }finally {
            // 将取出的pg重新插入pIndex
            if (pg != null){
                pageIndex.add(pi.pageNumber, PageX.getFreeSpace(pg));
            }else{
                pageIndex.add(pi.pageNumber, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem dataItem){
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di){
        super.release(di.getUid());
    }

    // 在创建文件时初始化PageOne
    void initPageOne(){
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        }catch (Exception e){
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时读入PageOne，并验证正确性
    boolean loadCheckPageOne(){
        try {
            pageOne = pc.getPage(1);
        }catch (Exception e){
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex(){
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            }catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
}
