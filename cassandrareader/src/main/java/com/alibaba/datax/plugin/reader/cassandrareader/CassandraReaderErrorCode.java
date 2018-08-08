package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.datastax.driver.core.Cluster;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Zane
 * @date 2018/8/8 下午6:17
 */
public enum CassandraReaderErrorCode implements ErrorCode {

    AUTHENTICATION_ERROR("Authentication error", "授权失败，请检查配置。"),
    COLUMNS_IS_EMPTY("Columns is empty", "columns 不能为空。");

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
}
