package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static com.alibaba.datax.plugin.reader.cassandrareader.CassandraReaderErrorCode.COLUMNS_IS_EMPTY;

/**
 * Cassandra 数据库中间件
 * <p>
 * 由于当前驱动是异步响应，而且默认有连接池处理，所以可以直接全局单例使用客户端对象。
 *
 * @author Zane
 * @date 2018/8/8 下午6:30
 */
public class CassandraUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraUtil.class);
    private static Cluster cluster;
    private static Session session;

    public static void init(Configuration configuration) {
        List<String> nodeList = configuration.getList(Key.NODES, String.class);

        String[] nodes = nodeList.toArray(new String[nodeList.size()]);
        Integer port = configuration.getInt(Key.PORT);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);

        init(nodes, port, username, password);
    }

    public static void init(String[] nodes, Integer port, String username, String password) {
        if (session != null) {
            return;
        }
        // 暂时使用默认池设置
        //PoolingOptions poolingOptions = new PoolingOptions()
        //        .setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
        //        .setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
        //        .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
        //        .setMaxConnectionsPerHost(HostDistance.REMOTE, 4);

        Cluster.Builder b = Cluster.builder()
                //.withPoolingOptions(poolingOptions)
                .addContactPoints(nodes);
        if (port != null) {
            b.withPort(port);
        }
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            b.withCredentials(username, password);
        }


        try {
            cluster = b.build();
            session = cluster.connect();
            cluster.getMetadata().getKeyspaces();
            LOG.info("cassandra init success");
        } catch (AuthenticationException ex) {
            throw DataXException.asDataXException(CassandraReaderErrorCode.AUTHENTICATION_ERROR, ex);
        }
    }


    /**
     * 执行查询
     *
     * @param cql cassandra query language
     * @return 数据行的枚举器
     */
    public static Iterator<Row> executeQuery(String cql) {
        ResultSet resultSet = session.execute(cql);
        return resultSet.iterator();
    }

    /**
     * 拼装查询语句
     *
     * @param configuration 配置
     * @param columns       字段列表
     * @return
     */
    public static String buildCql(Configuration configuration, List<JSONObject> columns) {
        if (columns == null || columns.isEmpty()) {
            throw DataXException.asDataXException(COLUMNS_IS_EMPTY, COLUMNS_IS_EMPTY.getDescription());
        }
        StringBuilder cql = new StringBuilder();
        cql.append("select ");
        for (JSONObject column : columns) {
            cql.append(column.getString("name"));
            cql.append(",");
        }
        cql.deleteCharAt(cql.length() - 1);

        cql.append(" from ");
        cql.append(configuration.getString(Key.KEYSPACE));
        cql.append(".");
        cql.append(configuration.getString(Key.TABLE));

        String result = cql.toString();
        LOG.info("buildCql:" + result);
        return result;
    }

    public static void close() {
        session.close();
        cluster.close();
        LOG.info("cassandra closed");
    }
}