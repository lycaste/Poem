
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TwoStreamsJoin {




    public static void main(String[] args) throws Exception {







        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KeyedStream<OrderEvent, String> orderStream = env
                .fromElements(
                        new OrderEvent("order_1", "pay", System.currentTimeMillis()))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.minutes(10)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.eventTime;
                    }
                })
                .keyBy(r -> r.orderId);

        KeyedStream<PayEvent, String> payStream = env
                .fromElements(
                        new PayEvent("order_2", "weixin", System.currentTimeMillis())
                )
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<PayEvent>(Time.minutes(5)) {
                    @Override
                    public long extractTimestamp(PayEvent element) {
                        return element.eventTime;
                    }
                })
                .keyBy(r -> r.orderId);

        SingleOutputStreamOperator<String> result = orderStream
                .connect(payStream)
                .process(new CoProcessFunction<OrderEvent, PayEvent, String>() {
                    private ValueState<OrderEvent> orderState;
                    private ValueState<PayEvent> payState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        orderState = getRuntimeContext().getState(
                                new ValueStateDescriptor<OrderEvent>("order", OrderEvent.class)
                        );
                        payState = getRuntimeContext().getState(
                                new ValueStateDescriptor<PayEvent>("pay", PayEvent.class)
                        );
                    }

                    @Override
                    public void processElement1(OrderEvent orderEvent, Context context, Collector<String> collector) throws Exception {
                        PayEvent pay = payState.value();
                        if (pay != null) {
                            payState.clear();
                            collector.collect("order id " + orderEvent.orderId + " matched success");
                        } else {
                            orderState.update(orderEvent);
                            context.timerService().registerEventTimeTimer(orderEvent.eventTime + 500000L);
                        }
                    }

                    @Override
                    public void processElement2(PayEvent payEvent, Context context, Collector<String> collector) throws Exception {
                        OrderEvent order = orderState.value();
                        if (order != null) {
                            orderState.clear();
                            collector.collect("order id" + payEvent.orderId + " matched success");
                        } else {
                            payState.update(payEvent);
                            context.timerService().registerEventTimeTimer(payEvent.eventTime + 500000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("@@@@@"+ ctx.timerService().currentWatermark());
                        if (orderState.value() != null) {
                           System.out.println("触发了timer");
                        }
                        if (payState.value() != null) {
                            System.out.println("触发了timer");

                        }
                    }
                });

        result.print();

        env.execute();
    }


    public static class OrderEvent {
        public String orderId;
        public String eventType;
        public Long eventTime;
        public Long timestamp;

        public OrderEvent(String orderId, String eventType, Long eventTime) {
            this.orderId = orderId;
            this.eventType = eventType;
            this.eventTime = eventTime;
            this.timestamp = eventTime;
        }

        public OrderEvent() { }

    }


    public static class PayEvent {
        public String orderId;
        public String eventType;
        public Long eventTime;
        public Long timestamp;
        public PayEvent(String orderId, String eventType, Long eventTime) {
            this.orderId = orderId;
            this.eventType = eventType;
            this.eventTime = eventTime;
            this.timestamp = eventTime;

        }

        public PayEvent() {
        }

    }
}
