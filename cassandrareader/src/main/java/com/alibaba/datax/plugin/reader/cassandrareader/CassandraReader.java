package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

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
            return CollectionSplitUtil.doSplit(this.originalConfig, adviceNumber);
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
        private List<JSONObject> columns;

        @Override
        public void startRead(RecordSender recordSender) {
            String cql = CassandraUtil.buildCql(this.taskConfig, this.columns);
            Iterator<Row> rowIterator = CassandraUtil.executeQuery(cql);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Record record = buildOneRecord(recordSender, row);
                recordSender.sendToWriter(record);
            }
        }

        private Record buildOneRecord(RecordSender recordSender, Row row) {
            Record record = recordSender.createRecord();
            record.addColumn(new StringColumn(row.toString()));
            return record;
        }

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.columns = this.taskConfig.getList(Key.COLUMNS, JSONObject.class);
        }

        @Override
        public void destroy() {

        }
    }
}
