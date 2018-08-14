package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.datax.plugin.reader.cassandrareader.CassandraReaderErrorCode.COLUMN_NAME_ERROR;

/**
 * @author Zane
 * @date 2018/8/8 下午6:17
 */
public class CassandraReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;
        private static Cluster cluster;

        /**
         * 0: 全局准备工作
         */
        @Override
        public void prepare() {
            super.prepare();
            CassandraUtil.init(super.getPluginJobConf());
        }

        /**
         * 1: 初始化 Job
         */
        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }


        /**
         * 2: 任务分片
         */
        @Override
        public List<Configuration> split(int adviceNumber) {
            return Util.splitJob(this.originalConfig, adviceNumber);
        }


        /**
         * 3: 销毁 Job
         */
        @Override
        public void destroy() {

        }

        /**
         * 4: 全局的后置工作
         */
        @Override
        public void post() {
            super.post();
            CassandraUtil.close();
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig = null;
        private Map<String, CassandraUtil.CassColumn> columns;

        @Override
        public void startRead(RecordSender recordSender) {
            Statement cql = CassandraUtil.buildQueryStatement(this.taskConfig, this.columns);
            ResultSet resultSet = CassandraUtil.executeQuery(cql);
            int count = 0;
            for (Row row : resultSet) {
                Record record = buildOneRecord(recordSender, row);
                recordSender.sendToWriter(record);
                count++;
                //LOG.info(Thread.currentThread().getName() + ":" + count);
            }

        }

        private Record buildOneRecord(RecordSender recordSender, Row row) {
            Record record = recordSender.createRecord();
            for (CassandraUtil.CassColumn column : columns.values()) {
                record.addColumn(CassandraUtil.buildOneColumn(row, column));
            }
            return record;
        }


        @Override
        public void init() {
            LOG.info("Task inited");
            this.taskConfig = super.getPluginJobConf();
            Map<String, CassandraUtil.CassColumn> tableColumns = CassandraUtil.getTableColumns(this.taskConfig.getString(Key.KEYSPACE), this.taskConfig.getString(Key.TABLE));

            // 当配置中有配置字段时，输出配置指定的字段，否则输出全部字段。
            List<JSONObject> configColumns = taskConfig.getList(Key.COLUMNS, JSONObject.class);
            if (configColumns == null || configColumns.isEmpty()) {
                this.columns = tableColumns;
            } else {
                this.columns = new HashMap<>();
                for (JSONObject column : configColumns) {
                    String name = column.getString(Key.COLUMN_NAME);
                    if (tableColumns.containsKey(name)) {
                        this.columns.put(name, tableColumns.get(name));

                    } else {
                        throw DataXException.asDataXException(COLUMN_NAME_ERROR, COLUMN_NAME_ERROR.getDescription(name));
                    }
                }
            }

        }

        @Override
        public void destroy() {
            LOG.info(Thread.currentThread().getName() + ": task done");
        }
    }
}
