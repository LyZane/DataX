package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AuthenticationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.datax.plugin.reader.cassandrareader.CassandraReaderErrorCode.*;

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
    private static Session session;

    public static void init(Configuration configuration) {
        List<String> nodeList = configuration.getList(Key.CONNECTION_NODES, String.class);

        String[] nodes = nodeList.toArray(new String[nodeList.size()]);
        Integer port = configuration.getInt(Key.CONNECTION_PORT);
        String username = configuration.getString(Key.CONNECTION_USERNAME);
        String password = configuration.getString(Key.CONNECTION_PASSWORD);

        init(nodes, port, username, password);
    }

    public static void init(String[] nodes, Integer port, String username, String password) {
        if (session != null) {
            return;
        }
        // 暂时使用默认池设置
        PoolingOptions poolingOptions = new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
                .setCoreConnectionsPerHost(HostDistance.REMOTE, 4)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, 10);

        Cluster.Builder b = Cluster.builder()
                .withPoolingOptions(poolingOptions)
                .addContactPoints(nodes);
        if (port != null) {
            b.withPort(port);
        }
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            b.withCredentials(username, password);
        }


        try {
            Cluster cluster = b.build();
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
    public static ResultSet executeQuery(String cql) {
        return executeQuery(new SimpleStatement(cql));
    }

    public static ResultSet executeQuery(Statement statement) {
        try {
            return session.executeAsync(statement).get(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw DataXException.asDataXException(CASSANDRA_EXECUTE_INTERRUPTED, CASSANDRA_EXECUTE_INTERRUPTED.getDescription(statement + "fetchSize:" + statement.getFetchSize()), e);
        } catch (TimeoutException e) {
            throw DataXException.asDataXException(CASSANDRA_EXECUTE_TIMEOUT, CASSANDRA_EXECUTE_TIMEOUT.getDescription(statement + "fetchSize:" + statement.getFetchSize()), e);
        } catch (ExecutionException e) {
            throw DataXException.asDataXException(CASSANDRA_CQL_ERROR, CASSANDRA_CQL_ERROR.getDescription(statement + "fetchSize:" + statement.getFetchSize()), e);
        }
    }

    /**
     * 拼装查询语句
     *
     * @param configuration 配置
     * @param columns       字段列表
     * @return
     */
    public static Statement buildQueryStatement(Configuration configuration, Map<String, CassColumn> columns) {
        String partitionWhere = configuration.getString(Key.WHERE_PARTITION_WHERE);
        String rangeColumn = configuration.getString(Key.WHERE_RANGE_COLUMN);
        String clusterFrom = configuration.getString(Key.WHERE_RANGE_FROM);
        String clusterTo = configuration.getString(Key.WHERE_RANGE_TO);

        String cql = String.format("select %s from %s.%s",
                columns == null || columns.isEmpty() ? "*" : StringUtils.join(columns.keySet(), ","),
                configuration.getString(Key.KEYSPACE),
                configuration.getString(Key.TABLE)
        );

        List<String> whereList = new ArrayList<>();

        if (!StringUtils.isBlank(partitionWhere)) {
            whereList.add(partitionWhere);
        }

        if (!StringUtils.isBlank(clusterFrom)) {
            whereList.add(String.format("%s >= %s", rangeColumn, clusterFrom));
        }

        if (!StringUtils.isBlank(clusterTo)) {
            whereList.add(String.format("%s < %s", rangeColumn, clusterTo));
        }

        if (!whereList.isEmpty()) {
            cql += " where " + StringUtils.join(whereList, " and ");

            if (configuration.getBool(Key.WHERE_ALLOW_FILTERING, false)) {
                cql += " ALLOW FILTERING";
            }
        }

        Statement result = new SimpleStatement(cql + ";");
        result.setFetchSize(configuration.getInt(Key.FETCH_SIZE, 100));

        LOG.info("buildQueryStatement:" + result);
        return result;
    }


    /**
     * 获取表定义的全部字段列表
     *
     * @param keyspace 库名
     * @param table    表名
     * @return
     */
    public static Map<String, CassColumn> getTableColumns(String keyspace, String table) {
        String cql = String.format("select * from %s.%s limit 1", keyspace, table);
        ResultSet resultSet = executeQuery(cql);
        Map<String, CassColumn> result = new HashMap<String, CassColumn>();
        for (ColumnDefinitions.Definition column : resultSet.getColumnDefinitions().asList()) {
            result.put(column.getName(), new CassColumn(column.getName(), column.getType().getName().name()));
        }
        return result;
    }

    /**
     * 将 cassandra 字段映射为 dataX 的字段
     *
     * @param row
     * @param cassColumn
     * @return
     */
    public static Column buildOneColumn(Row row, CassandraUtil.CassColumn cassColumn) {
        String name = cassColumn.name;
        switch (cassColumn.type.toLowerCase()) {
            case "ascii":
            case "inet":
            case "text":
            case "timeuuid":
            case "uuid":
            case "varchar":
                return new StringColumn(row.getString(name));
            case "int":
            case "smallint":
            case "tinyint":
            case "varint":
                return new LongColumn(row.getInt(name));
            case "counter":
            case "time":
            case "bigint":
                return new LongColumn(row.getLong(name));
            case "timestamp":
                return new DateColumn(row.getTimestamp(name));
            case "decimal":
            case "double":
            case "float":
                return new DoubleColumn(row.getDouble(name));
            case "blob":
                return new BytesColumn(row.getBytes(name).array());
            case "date":
                return new DateColumn(row.getDate(name).getMillisSinceEpoch());
            case "boolean":
                return new BoolColumn(row.getBool(name));
            case "map":
                // 这里暂时将 map 的 k/v 都映射为 String，如果需要自动识别为其他类型，需要扩展功能。
                return new StringColumn(JSON.toJSONString(row.getMap(name, String.class, String.class)));
            default:
                throw DataXException.asDataXException(UNSUPPORTED_DATA_TYPE, UNSUPPORTED_DATA_TYPE.getDescription(cassColumn.type));

        }
    }

    public static void close() {
        Cluster cluster = session.getCluster();
        session.close();
        cluster.close();
        LOG.info("cassandra closed");
    }

    static class CassColumn {
        public String name;
        public String type;

        public CassColumn(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String toString() {
            return this.name + ":" + this.type;
        }
    }
}