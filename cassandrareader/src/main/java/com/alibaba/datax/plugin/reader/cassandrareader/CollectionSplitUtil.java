package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 任务拆分工具类
 *
 * @author Zane
 * @date 2018/8/8 下午9:01
 */
public class CollectionSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CollectionSplitUtil.class);

    /**
     * 任务切割
     * <p>
     * 情形一：没有设置聚集索引时，全表扫描，不能切割任务。
     * 情形二：只设置了聚集索引的终点，没有设置起点，查询到起点值之后变为情形三。
     * 情形三：聚集索引设置了起点和终点时按 adviceNumber 数量进行切割。
     *
     * @param originalConfig 配置
     * @param adviceNumber   推荐的分片数
     * @return
     */
    public static List<Configuration> doSplit(Configuration originalConfig, int adviceNumber) {
        LOG.info("adviceNumber:" + adviceNumber);

        List<Configuration> result = new ArrayList<Configuration>();
        Configuration clone = originalConfig.clone();

        result.add(clone);

        return result;
    }

    class Range {
        Object from;
        Object to;
    }
}
