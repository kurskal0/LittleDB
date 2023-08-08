package gzhu.la.littledb.backend.dm.page;

import gzhu.la.littledb.backend.dm.pageCache.PageCache;
import gzhu.la.littledb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {

    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    // 启动时设置初始字节：
    public static void setVcOpen(Page pg){
        pg.setDirty(true);  // 对页面有修改时就设置为脏数据，然后flush到磁盘中
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw){
        // LEN_VC 8  OF_VC 100
        // 原数组  原数组开始  目标数组  目标数组开始  截取的长度
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    // 关闭时拷贝字节：
    public static void setVcClose(Page pg){
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw){
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    // 校验字节：
    public static boolean checkVc(Page pg){
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw){
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + LEN_VC * 2));
    }
}
