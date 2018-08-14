package com.alibaba.datax.plugin.reader.cassandrareader;


import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

/**
 * @author Zane
 * @date 2018/8/8 下午6:36
 */
public class UtilTest {

    @Test
    public void doDateSplit() {
        List<Util.Range> list = Util.doSplit("2018-08-13 10:20", "2018-08-13 11:20", new CassandraUtil.CassColumn("log_time", "timestamp"), 10);
        for (Util.Range range : list) {
            System.out.println(range.from + "-" + range.to);
        }
    }

    @Test
    public void doBigIntegerSplit() {

        BigInteger[] list = Util.doBigIntegerSplit(BigInteger.valueOf(0), BigInteger.valueOf(9), 3);
        for (int i = 1; i < list.length; i++) {
            System.out.println(list[i - 1] + "-" + list[i]);
        }
    }


    @Test
    public void str2Date() {

        String[] datas = {"2018-8-10", "2018-8-11 10", "2018-8-11 11:38", "2018-8-12 12:38:12"};
        for (String data : datas) {
            System.out.println(Util.str2Date(data, true));
        }
    }


}