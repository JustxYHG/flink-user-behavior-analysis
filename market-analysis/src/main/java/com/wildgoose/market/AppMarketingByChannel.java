package com.wildgoose.market;

import com.wildgoose.market.beans.ChannelPromotionCount;
import com.wildgoose.market.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @Description : APP市场推广统计 - 分渠道
 * @Author : JustxzzZ
 * @Date : 2023.05.02 17:16
 */
public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.从自定义数据源中读取数据，并指定 eventTime 字段
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        // TODO 3.APP市场推广
        // 分渠道开窗统计
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingCountAgg(), new MarketingCountWindow());

        // 4.输出结果
        resultStream.print("app marketing count");

        // 5.执行
        env.execute("app marketing count job");

    }


    /**
     * 自定义数据源，模拟 MarketingUserBehavior 数据
     */
    private static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {

        private Boolean running = true;

        // 定义用户行为和渠道范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo", "browser");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

            while (running) {
                // 随机生成所有字段
                Long userId = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();

                // 发出数据
                ctx.collect(new MarketingUserBehavior(userId, behavior, channel, timestamp));

                Thread.sleep(100L);
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    /**
     * 自定义聚合函数，对不同渠道不同行为的数据进行统计
     */
    private static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }


    /**
     * 自定义窗口函数，获取不同渠道不同行为的统计值及窗口信息，并包装成 POJO
     */
    private static class MarketingCountWindow implements WindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple,
                          TimeWindow window, Iterable<Long> input,
                          Collector<ChannelPromotionCount> out) throws Exception {

            // 格式化时间戳
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String windowEndFormat = simpleDateFormat.format(new Date(window.getEnd()));

            out.collect(new ChannelPromotionCount(tuple.getField(0), tuple.getField(1), windowEndFormat, input.iterator().next()));
        }
    }
}
