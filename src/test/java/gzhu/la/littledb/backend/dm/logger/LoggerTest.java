package gzhu.la.littledb.backend.dm.logger;

import org.junit.Test;

import java.util.Arrays;

public class LoggerTest {

    @Test
    public void test(){
        Logger logger = Logger.create("D:\\tool\\LittleDB\\dm\\logger\\log");
        byte[] b = new byte[128];
        logger.log(b);
        byte[] log = logger.next();
        logger.close();
        System.out.println(Arrays.toString(log));
    }
}
