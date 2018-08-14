package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 工具类
 *
 * @author Zane
 * @date 2018/8/8 下午9:01
 */
public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private static final Pattern PATTERN_MATCH_DATE = Pattern.compile("^(?<year>\\d{4})[^\\d](?<month>\\d{1,2})[^\\d](?<day>\\d{1,2})[^\\d]?[^\\d]?((?<hour>\\d{1,2})([^\\d](?<minute>\\d{1,2})([^\\d](?<second>\\d{1,2})[^\\d]?)?)?)?$");
    private static final SimpleDateFormat DATE_TO_STRING_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


    /**
     * 任务切割
     * <p>
     * 情形一：没有设置聚集索引时，全表扫描，不能切割任务。
     * 情形二：聚集索引设置了起点和终点时按 adviceNumber 数量进行切割。
     *
     * @param originalConfig 配置
     * @param adviceNumber   推荐的分片数
     * @return
     */
    public static List<Configuration> splitJob(Configuration originalConfig, int adviceNumber) {
        String keyspace = originalConfig.getString(Key.KEYSPACE);
        String table = originalConfig.getString(Key.TABLE);
        String clusterFrom = originalConfig.getString(Key.WHERE_RANGE_FROM);
        String clusterTo = originalConfig.getString(Key.WHERE_RANGE_TO);


        List<Configuration> result = new ArrayList<Configuration>();
        // 情形一
        if (StringUtils.isBlank(clusterFrom) && StringUtils.isBlank(clusterTo)) {
            Configuration clone = originalConfig.clone();
            result.add(clone);
            return result;
        }

        // 情形二
        if (!StringUtils.isBlank(clusterFrom) && !StringUtils.isBlank(clusterTo)) {

            Map<String, CassandraUtil.CassColumn> tableColumns = CassandraUtil.getTableColumns(keyspace, table);
            String clusterColumn = originalConfig.getString(Key.WHERE_RANGE_COLUMN);
            if (!tableColumns.containsKey(clusterColumn)) {
                throw new IllegalArgumentException(String.format("错误的聚集索引字段: %s.%s 中没有字段 %s", keyspace, table, clusterColumn));
            }

            List<Range> ranges = doSplit(clusterFrom, clusterTo, tableColumns.get(clusterColumn), adviceNumber);
            for (Range range : ranges) {
                Configuration clone = originalConfig.clone();
                clone.set(Key.WHERE_RANGE_FROM, range.from);
                clone.set(Key.WHERE_RANGE_TO, range.to);
                LOG.info("slice config[" + result.size() + "]:" + clone);
                result.add(clone);
            }

            return result;
        }

        throw new IllegalArgumentException("where.range 中的 from he to 不能只设置其中一个。当 from 和 to 都不设置时，进行单线程全表读取；当 from 和 to 都设置时，会进行多线程分片读取。");
    }


    public static List<Range> doSplit(String fromStr, String toStr, CassandraUtil.CassColumn cassColumn, int sliceCount) {

        String type = cassColumn.type.toLowerCase();
        BigInteger from, to;
        List<Range> result = new ArrayList<>();
        switch (type) {
            case "ascii":
            case "inet":
            case "text":
            case "timeuuid":
            case "uuid":
            case "varchar":
                throw new IllegalArgumentException("暂不支持按字符串类型的字段进行分片");
            case "blob":
                throw new IllegalArgumentException("暂不支持按 blob 类型的字段进行分片");
            case "int":
            case "smallint":
            case "tinyint":
            case "varint":
            case "counter":
            case "time":
            case "bigint":

            case "decimal":
            case "double":
            case "float":
                from = BigInteger.valueOf(Long.parseLong(fromStr));
                to = BigInteger.valueOf(Long.parseLong(toStr));

                // 因为小数部分会在 parseLong 时被丢失，所以对 from 和 to 分别加减 1 进行补偿。
                switch (type) {
                    case "decimal":
                    case "double":
                    case "float":
                        from = from.subtract(BigInteger.ONE);
                        to = to.add(BigInteger.ONE);
                    default:
                }

                BigInteger[] list1 = doBigIntegerSplit(from, to, sliceCount);
                for (int i = 1; i < list1.length; i++) {
                    result.add(Range.create(list1[i - 1], list1[i]));
                }
                return result;


            case "timestamp":
            case "date":
                Date fromDate = str2Date(fromStr);
                Date toDate = str2Date(toStr, true);
                from = BigInteger.valueOf(fromDate.getTime());
                to = BigInteger.valueOf(toDate.getTime());
                BigInteger[] list2 = doBigIntegerSplit(from, to, sliceCount);
                for (int i = 1; i < list2.length; i++) {
                    result.add(
                            Range.create(
                                    "'" + DATE_TO_STRING_FORMAT.format(new Date(list2[i - 1].longValue())) + "'",
                                    "'" + DATE_TO_STRING_FORMAT.format(new Date(list2[i].longValue())) + "'"
                            )
                    );
                }
                return result;


            default:
                throw new IllegalArgumentException("暂不支持按 " + cassColumn.type + " 类型的字段进行分片");

        }

    }

    /**
     * 将字符串格式化为 Date
     *
     * @param str 字符串形式的时间
     * @return
     */
    public static Date str2Date(String str) {
        return str2Date(str, false);
    }

    /**
     * 将字符串格式化为 Date
     *
     * @param str       字符串形式的时间
     * @param isEndTime 是否是结束时间，如果是，会用当天最后时刻的对应值填充缺省单位。
     * @return
     */
    public static Date str2Date(String str, boolean isEndTime) {
        Matcher matcher = PATTERN_MATCH_DATE.matcher(str);
        if (!matcher.find()) {
            throw new IllegalArgumentException("解析为 Date 失败，当前输入为：" + str);
        }

        int year = Integer.valueOf(matcher.group("year"));
        int month = Integer.valueOf(matcher.group("month"));
        int day = Integer.valueOf(matcher.group("day"));

        int hour = matcher.group("hour") != null ? Integer.valueOf(matcher.group("hour")) : (isEndTime ? 23 : 0);
        int minute = matcher.group("minute") != null ? Integer.valueOf(matcher.group("minute")) : (isEndTime ? 59 : 0);
        int second = matcher.group("second") != null ? Integer.valueOf(matcher.group("second")) : (isEndTime ? 59 : 0);

        return new Date(year - 1900, month - 1, day, hour, minute, second);
    }

    /**
     * 对数字类型的范围区间进行分片，最终分片数量可能比期望的分片数大1。
     *
     * @param from       起点
     * @param to         终点
     * @param sliceCount 期望的分片数
     * @return 返回的线段中，终点会比传入的终点大1，方便外部统一使用 >= && < 的方式进行区间限定。
     */
    public static BigInteger[] doBigIntegerSplit(BigInteger from, BigInteger to, int sliceCount) {
        from = from.subtract(BigInteger.valueOf(1534100000000L));
        to = to.subtract(BigInteger.valueOf(1534100000000L));
        if (sliceCount < 1) {
            throw new IllegalArgumentException(String.format("切分份数不能小于1. sliceCount=[%s].", sliceCount));
        }

        if (null == from || null == to) {
            throw new IllegalArgumentException(String.format("对 BigInteger 进行切分时，其左右区间不能为 null. left=[%s],right=[%s].", from, to));
        }

        // 调整大小顺序，确保 from < to
        if (from.compareTo(to) > 0) {
            BigInteger temp = from;
            from = to;
            to = temp;
        }

        to = to.add(BigInteger.ONE);

        if (from.compareTo(to) == 0 || sliceCount == 1) {
            return new BigInteger[]{from, to};
        }

        // 总区间
        BigInteger gap = to.subtract(from);
        // 每个分片的大小
        BigInteger step = gap.divide(BigInteger.valueOf(sliceCount));
        // 余数
        BigInteger remainder = gap.remainder(BigInteger.valueOf(sliceCount));


        if (step.compareTo(BigInteger.ZERO) == 0) {
            sliceCount = remainder.intValue();
        }

        List<BigInteger> result = new ArrayList<>();
        result.add(from);

        BigInteger bound = from;
        for (int i = 1; i <= sliceCount; i++) {
            bound = bound.add(step);
            result.add(bound);
        }

        if (remainder.compareTo(BigInteger.ZERO) > 0) {
            bound = bound.add(remainder);
            result.add(bound);
        }

        return result.toArray(new BigInteger[result.size()]);

    }


    static class Range {
        public String from;
        public String to;

        public static Range create(Object from, Object to) {
            return new Range(from.toString(), to.toString());
        }

        public Range(String from, String to) {
            this.from = from;
            this.to = to;
        }
    }
}
