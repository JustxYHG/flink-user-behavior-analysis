package com.wildgoose.loginfaildetect;

import com.wildgoose.loginfaildetect.beans.LoginEvent;
import com.wildgoose.loginfaildetect.beans.LoginFailWarning;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @Description : 失败登录检测 - 同一用户（可以是不同IP）在2秒内连续两次登录失败，需要报警
 * @Author : JustxzzZ
 * @Date : 2023.05.04 14:31
 */
public class LoginFailWithCEP {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1.从文件中读取数据
        String filePath = LoginFailWithCEP.class.getResource("/LoginLog.csv").getPath();
        DataStreamSource<String> inputStream = env.readTextFile(filePath);

        // 2.转换成 POJO ，并指定 eventTime 和 watermark
        SingleOutputStreamOperator<LoginEvent> loginEventStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // TODO 3.恶意登录检测
        // 3.1 定义一个匹配模式：firstFail -> secondFail, within 2s
        // 要扩展时只需要追加 .next()，然后再 PatternSelectFunction 中对 lastFailEvent 取最后一个 next 即可
        Pattern<LoginEvent, LoginEvent> loginFailPattern0 = Pattern
                .<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).next("secondFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).within(Time.seconds(2));

        // TODO 采用循环个体模式优化匹配模式，便于扩展
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("failEvents").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).times(3).consecutive().within(Time.seconds(5));

        // 3.2 将匹配模式应用到数据流上，得到一个 pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), loginFailPattern);

        // 3.3 检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMatchDetectWarning());

        // 4.输出结果
        warningStream.print("warning");

        env.execute("login fail detect with cep job");

    }


    /**
     * 自定义 PatternSelectFunction 函数，检出符合模式的事件进行报警
     */
    private static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent, LoginFailWarning> {

        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
//            LoginEvent firstFailEvent = map.get("firstFail").get(0);
//            LoginEvent LastFailEvent = map.get("secondFail").get(0);
//            return new LoginFailWarning(
//                    firstFailEvent.getUserId(),
//                    firstFailEvent.getTimestamp(),
//                    LastFailEvent.getTimestamp(),
//                    "login fail 2 times.");

            List<LoginEvent> failEvents = map.get("failEvents");
            int failEventCount = failEvents.size();

            LoginEvent firstFailEvent = failEvents.get(0);
            LoginEvent lastFailEvent = failEvents.get(failEventCount - 1);

            return new LoginFailWarning(
                    firstFailEvent.getUserId(),
                    firstFailEvent.getTimestamp(),
                    lastFailEvent.getTimestamp(),
                    "login fail " + failEventCount + " times.");

        }
    }
}