package com.wildgoose.market;

import com.wildgoose.market.beans.AdClickEvent;
import com.wildgoose.market.beans.AdCountViewByProvince;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;

/**
 * @Description : 页面广告分析
 * @Author : JustxzzZ
 * @Date : 2023.05.02 19:23
 */
public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1.从文件中读取数据
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 2.转换成 POJOs，并指定 eventTime 和 watermark
        SingleOutputStreamOperator<AdClickEvent> adClickEventStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // TODO 3.广告推广统计分析
        // 基于省份开窗分组统计 Slide(1h, 5s)
        SingleOutputStreamOperator<AdCountViewByProvince> adCountStream = adClickEventStream
                .keyBy("province")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdCountAgg(), new AdCountWindow());

        // 4.输出结果
        adCountStream.print("adCount");

        env.execute("ad count job");

    }


    /**
     * 自定义聚合函数，根据省份统计广告推广数量
     */
    private static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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
     * 自定义窗口函数，获取各省份广告推广数量及窗口信息，并包装成 POJO
     */
    private static class AdCountWindow implements WindowFunction<Long, AdCountViewByProvince, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {

            String windowEnd = new Timestamp(window.getEnd()).toString();
            out.collect(new AdCountViewByProvince(tuple.getField(0), windowEnd, input.iterator().next()));

        }
    }
}
