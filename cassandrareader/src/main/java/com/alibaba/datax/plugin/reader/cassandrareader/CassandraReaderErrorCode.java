package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author Zane
 * @date 2018/8/8 下午6:17
 */
public enum CassandraReaderErrorCode implements ErrorCode {

    AUTHENTICATION_ERROR("Authentication error", "授权失败，请检查配置。"),
    COLUMN_NAME_ERROR("Column name error", "错误的字段名称：%s"),
    UNSUPPORTED_DATA_TYPE("Unsupported data type", "暂不支持的数据类型：%s"),
    CASSANDRA_CQL_ERROR("Cassandra cql error", "错误的 cql 语句：%s"),
    CASSANDRA_EXECUTE_TIMEOUT("Cassandra execute timeout", "cql 执行超时：%s"),
    CASSANDRA_EXECUTE_INTERRUPTED("Cassandra execute interrupted", "cql 执行时被意外中断：%s");


    private final String code;
    private final String description;

    private CassandraReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    public String getDescription(String... args) {
        return String.format(this.description, args);
    }
}
