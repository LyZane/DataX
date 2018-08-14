package com.alibaba.datax.plugin.reader.cassandrareader;

/**
 * @author Zane
 * @date 2018/8/8 下午8:00
 */
public class Key {

    //region （job.json 中的配置项）

    public final static String CONNECTION_NODES = "connection.nodes";
    public final static String CONNECTION_PORT = "connection.port";
    public final static String CONNECTION_USERNAME = "connection.username";
    public final static String CONNECTION_PASSWORD = "connection.password";


    public final static String WHERE_PARTITION_WHERE = "where.partitionWhere";
    public final static String WHERE_RANGE_COLUMN = "where.range.column";
    public final static String WHERE_RANGE_FROM = "where.range.from";
    public final static String WHERE_RANGE_TO = "where.range.to";
    public final static String WHERE_RANGE_TYPE = "where.range.type";
    public final static String WHERE_ALLOW_FILTERING = "where.allowFiltering";

    public final static String KEYSPACE = "keyspace";
    public final static String TABLE = "table";
    public final static String FETCH_SIZE = "fetchSize";
    public final static String COLUMNS = "columns";
    public final static String COLUMN_NAME = "name";


    //endregion


    //public final static String SLICE_RECORD_COUNT = "sliceRecordCount";
}
