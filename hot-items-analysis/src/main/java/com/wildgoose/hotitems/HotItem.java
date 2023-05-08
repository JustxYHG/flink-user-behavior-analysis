package com.wildgoose.hotitems;

import com.wildgoose.hotitems.beans.ItemViewCount;
import com.wildgoose.hotitems.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

/**
 * @Description : 热门商品分析，实时返回浏览量前N的商品
 * @Author : JustxzzZ
 * @Date : 2023.04.25 11:13
 */
public class HotItem {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        // 2.读取数据，该数据严格顺序
//        DataStreamSource<String> inputStream =
//                env.readTextFile("hot-items-analysis/src/main/resources/UserBehavior.csv");

        // 用 kafka 作为数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> inputStream =
                env.addSource(new FlinkKafkaConsumer<String>("hot_items", new SimpleStringSchema(), properties));

        // 3.转换成POJOs，并指定 eventTime 字段
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

        // TODO 4.热门商品分析
        SingleOutputStreamOperator<ItemViewCount> itemCountStream = dataStream
                .filter(data -> data.getBehavior().equals("pv"))            // 筛选用户行为为点击的数据
                .keyBy("itemId")                                     // 根据商品ID分组
                .timeWindow(Time.minutes(60), Time.minutes(5))              // 开窗，步长1小时，滑动距离5分钟
                .aggregate(new ItemCountAgg(), new WindowResultFunction())  // 统计每个窗口中每种商品的访问量
                ;

        SingleOutputStreamOperator<String> hotItemStream = itemCountStream
                .keyBy("windowEnd")                                  // 根据窗口分组
                .process(new TopNHotItem(3))                         // 统计每个窗口中访问量前N的商品
                ;

        // 5.输出结果
        hotItemStream.print("HotItemTopN");

        // 6.执行
        env.execute("hot-item");

    }


    /**
     * 自定义预聚合函数，每来一个数据就 count + 1
     */
    private static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
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
     * 自定义窗口函数，结合窗口信息，输出当前 count 结果
     */
    private static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple,
                          TimeWindow window,
                          Iterable<Long> input,
                          Collector<ItemViewCount> out) throws Exception {

             out.collect(new ItemViewCount(tuple.getField(0), window.getEnd(), input.iterator().next()));

        }
    }

    /**
     * 自定义排序函数，输出热门商品 topN
     */
    private static class TopNHotItem extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private Integer topSize;
        // 状态变量，保存每个 itemViewCount
        private ListState<ItemViewCount> itemViewCountListState;

        public TopNHotItem(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取状态变量
            itemViewCountListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>("item-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value,
                                   KeyedProcessFunction<Tuple, ItemViewCount, String>.Context ctx,
                                   Collector<String> out) throws Exception {

            // 将到来的 itemViewCount 添加到状态变量
            itemViewCountListState.add(value);
            // 注册一个定时器，延迟1s触发
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);

        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Tuple, ItemViewCount, String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {

            // 将 ListState 转换成 List，方便排序
            List<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());

            // 定义一个输出结果字符串
            StringBuilder result = new StringBuilder();
            result.append("\n========================================\n");
            result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                result.append("No.").append(i + 1).append(": ").append("\t")
                        .append("商品ID=").append(itemViewCount.getItemId()).append("\t")
                        .append("浏览量=").append(itemViewCount.getCount()).append("\n");
            }

            result.append("========================================\n\n");

            Thread.sleep(1000);

            // 输出
            out.collect(result.toString());

        }
    }
}
