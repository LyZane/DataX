package com.alibaba.datax.plugin.reader.cassandrareader;


import com.datastax.driver.core.Row;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.util.Iterator;

/**
 * @author Zane
 * @date 2018/8/8 下午6:36
 */
public class CassandraConnectorTest {


    @Before
    public void init() {
        CassandraUtil.init(new String[]{"10.90.0.4"}, null, "wedefend", "Wolaidai2018");
    }
    @After
    public void close() {
        CassandraUtil.close();
    }

    @Test
    public void query() {
        Iterator<Row> iterator = CassandraUtil.executeQuery("select * from pangu.b_order limit 10;");

        while (iterator.hasNext()) {
            Row row = iterator.next();
            System.out.println(row);
        }

    }
}