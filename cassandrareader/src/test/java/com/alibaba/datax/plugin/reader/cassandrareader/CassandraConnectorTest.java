package com.alibaba.datax.plugin.reader.cassandrareader;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * @author Zane
 * @date 2018/8/8 下午6:36
 */
public class CassandraConnectorTest {


    @BeforeAll
    public static void init() {
        CassandraUtil.init(new String[]{""}, null, "", "");
    }

    @AfterAll
    public static void close() {
        CassandraUtil.close();
    }

    @Test
    public void query() {
        ResultSet resultSet = CassandraUtil.executeQuery("");
        for (Row row : resultSet) {
            System.out.println(row);
        }
    }

    @Test
    public void getTableColumns() {
        Map<String, CassandraUtil.CassColumn> tableColumns = CassandraUtil.getTableColumns("", "");
        for (CassandraUtil.CassColumn column : tableColumns.values()) {
            System.out.println(column);
        }
    }
}