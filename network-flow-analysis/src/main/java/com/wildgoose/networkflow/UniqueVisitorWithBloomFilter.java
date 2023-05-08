package com.wildgoose.networkflow;

import com.wildgoose.networkflow.beans.PageViewCount;
import com.wildgoose.networkflow.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * @Description : 实时流量分析 - 独立访客数 UV
 * @Author : JustxzzZ
 * @Date : 2023.04.28 15:24
 */
public class UniqueVisitorWithBloomFilter {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 2.读取数据源
        DataStreamSource<String> inputStream =
                env.readTextFile("network-flow-analysis/src/main/resources/UserBehavior.csv");

        // 3.转换成 POJOs ，并指定 eventTime 字段
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(
                    new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4])
            );
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // TODO 4.统计流量 - UV
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountWithBloomFilter());

        // 5.输出结果
        uvStream.print("pv");

        // 6.执行
        env.execute("network-flow-uv with bloom filter");

    }


    /**
     * 自定义触发器
     */
    private static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }
    }


    /**
     * 自定义布隆过滤器
     */
    public static class MyBloomFilter {
        // 定义位图的大小，一般需要定义为 2 的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 实现一个 hash 函数
        public Long hasCode(String value, Integer seed) {

            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);

        }
    }


    /**
     * 自定义全窗口函数类 - 统计页面 UV
     */
    private static class UvCountWithBloomFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        // 定义 jedis 连接和布隆过滤器
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102", 6379);
            myBloomFilter = new MyBloomFilter(1<<29);   // 要处理1亿个数据，用64M（此处用了位计算）
        }

        @Override
        public void process(ProcessAllWindowFunction<UserBehavior,
                PageViewCount, TimeWindow>.Context context, Iterable<UserBehavior> elements,
                            Collector<PageViewCount> out) throws Exception {

            // 将位图和窗口 count 值全部存入 redis
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();

            // 把 count 值存成一张 hash 表
            String uvCountHashName = "uv-count";
            String uvCountKey = windowEnd.toString();

            // 1.取当前的 userId
            String userId = elements.iterator().next().getUserId().toString();

            // 2.计算位图中的 offset
            Long offset = myBloomFilter.hasCode(userId, 61);

            // 3.用 redis 的 getbit 命令，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if (!isExist) {
                // 如果不存在，对应位图位置置为1
                jedis.setbit(bitmapKey, offset, true);

                // 更新 redis 中保存的 count 值
                Long uvCount = 0L;  // 初始 count 值
                String uvCountString = jedis.hget(uvCountHashName, uvCountKey);
                if (uvCountString != null && !"".equals(uvCountString)) {
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(uvCountHashName, uvCountKey, String.valueOf(uvCount + 1));

                out.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
            }
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
    }
