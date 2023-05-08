package com.wildgoose.loginfaildetect;

import com.wildgoose.loginfaildetect.beans.LoginEvent;
import com.wildgoose.loginfaildetect.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @Description : 失败登录检测 - 同一用户（可以是不同IP）在2秒内连续两次登录失败，需要报警
 * @Author : JustxzzZ
 * @Date : 2023.05.04 14:31
 */
public class LoginFail {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1.从文件中读取数据
        String filePath = LoginFail.class.getResource("/LoginLog.csv").getPath();
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
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        // 4.输出结果
        warningStream.print("warning");

        env.execute("login fail detect job");

    }


    /**
     * 自定义 process 函数，监测连续2s内的登录失败次数，并对超过阈值的用户进行报警
     */
    private static class LoginFailDetectWarning0 extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        private Integer maxFailTimes;                           // 定义属性，最大连续登录失败次数
        private ListState<LoginEvent> loginFailEventListState;  // 定义状态，保存2秒内所有登录失败事件
        private ValueState<Long> timerTsState;                  // 定义状态，保存注册的定时器时间戳

        public LoginFailDetectWarning0(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("login-fail-event-list", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer-ts", Long.class));
        }

        @Override
        public void processElement(LoginEvent value,
                                   KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.Context ctx,
                                   Collector<LoginFailWarning> out) throws Exception {

            // 如果当前是第一次登录失败，则注册一个2s后的定时器，并将登录事件保存到状态变量中；否则判断登录失败次数是否超过阈值
            // 判断当前登录事件状态
            if ("fail".equals(value.getLoginState())) {
                // 1.如果是失败事件，则添加到列表状态中
                loginFailEventListState.add(value);
                // 如果没有定时器，则注册一个2s后的定时器
                if (timerTsState.value() == null) {
                    Long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            } else {
                // 2.如果是成功事件，则注销定时器，清空状态
                if (timerTsState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                }

                loginFailEventListState.clear();
                timerTsState.clear();
            }

        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.OnTimerContext ctx,
                            Collector<LoginFailWarning> out) throws Exception {

            // 定时器触发，说明2s内没有成功登录，判断 listState 中的个数
            ArrayList<LoginEvent> loginFailEvents = new ArrayList<>();
            loginFailEventListState.get().iterator().forEachRemaining(loginFailEvents::add);
            Integer failTimes = loginFailEvents.size();

            if (failTimes >= maxFailTimes) {
                // 如果超出设定的最大失败次数，输出报警
                LoginEvent firstLoginFailEvent = loginFailEvents.get(0);
                LoginEvent lastLoginEvent = loginFailEvents.get(failTimes - 1);
                out.collect(new LoginFailWarning(
                        ctx.getCurrentKey(),
                        firstLoginFailEvent.getTimestamp(),
                        lastLoginEvent.getTimestamp(),
                        "login fail for " + failTimes + " times in 2 seconds"));
            }

            // 清空状态
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }


    /**
     * 自定义 process 函数，监测连续2s内的登录失败次数，并对超过阈值的用户进行报警，对事件模式进行监控，不再需要每次都等待2s
     * 优缺点：
     *      1. 优点：时效性提高了，一旦出现连续登录失败就报警，不再需要等待2s
     *      2. 缺点：失败次数阈值相关的处理逻辑已写死（此次只判断了连续登录失败2次的逻辑），扩展性差；并且解决不了乱序数据问题
     */
    private static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        private Integer maxFailTimes;                           // 定义属性，最大连续登录失败次数
        private ListState<LoginEvent> loginFailEventListState;  // 定义状态，保存2秒内所有登录失败事件

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("login-fail-event-list", LoginEvent.class));
        }

        // 以登录事件作为判断报警触发的条件，不再注册定时器
        @Override
        public void processElement(LoginEvent value,
                                   KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.Context ctx,
                                   Collector<LoginFailWarning> out) throws Exception {

            // 判断当前事件登录状态
            if ("fail".equals(value.getLoginState())) {
                // 1.如果是失败事件，获取状态中之前登录失败的事件，继续判断是否已有失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()) {
                    // 1.1 如果已经存在登录失败事件，继续判断时间戳是否在2s之内
                    // 获取已有的登录失败事件
                    LoginEvent firstFailEvent = iterator.next();
                    if (value.getTimestamp() - firstFailEvent.getTimestamp() <= 2) {
                        out.collect(new LoginFailWarning(
                                ctx.getCurrentKey(),
                                firstFailEvent.getTimestamp(),
                                value.getTimestamp(),
                                "login fail 2 times in 2 seconds."));
                    }

                    // 不管报不报警，这次都已处理完毕，直接更新状态
                    loginFailEventListState.clear();
                    loginFailEventListState.add(value);

                } else {
                    // 1.2 如果没有登录失败，直接将当前事件存入 ListState
                    loginFailEventListState.add(value);
                }
            } else {
                // 2.如果是成功事件，直接清空状态
                loginFailEventListState.clear();
            }

        }
    }
}