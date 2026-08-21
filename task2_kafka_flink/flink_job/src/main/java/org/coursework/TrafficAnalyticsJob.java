package org.coursework;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TrafficAnalyticsJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("traffic-telemetry")
                .setGroupId("traffic-flink-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Traffic Source"
        );

        ObjectMapper objectMapper = new ObjectMapper();

        DataStream<TrafficEvent> trafficStream = rawStream.map(
                new MapFunction<String, TrafficEvent>() {
                    @Override
                    public TrafficEvent map(String value) throws Exception {
                        return objectMapper.readValue(value, TrafficEvent.class);
                    }
                }
        );

        WatermarkStrategy<TrafficEvent> watermarkStrategy =
        WatermarkStrategy
                .<TrafficEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<TrafficEvent>() {
                            @Override
                            public long extractTimestamp(
                                    TrafficEvent event,
                                    long recordTimestamp
                            ) {
                                LocalDateTime dateTime =
                                        LocalDateTime.parse(event.read_date);

                                return dateTime
                                        .toInstant(ZoneOffset.UTC)
                                        .toEpochMilli();
                            }
                        }
                );

        DataStream<TrafficEvent> watermarkedStream =
        trafficStream.assignTimestampsAndWatermarks(
                watermarkStrategy
        );

        KeyedStream<TrafficEvent, Integer> keyedStream =
        watermarkedStream.keyBy(event -> event.atd_device_id);

        SingleOutputStreamOperator<TrafficWindowResult> aggregatedStream =
        keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(15)))
                .aggregate(
                        new AggregateFunction<TrafficEvent, Integer, Integer>() {

                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            @Override
                            public Integer add(
                                    TrafficEvent event,
                                    Integer accumulator
                            ) {
                                return accumulator + event.volume;
                            }

                            @Override
                            public Integer getResult(Integer accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Integer merge(
                                    Integer first,
                                    Integer second
                            ) {
                                return first + second;
                            }
                        },

                        new ProcessWindowFunction<
                                Integer,
                                TrafficWindowResult,
                                Integer,
                                TimeWindow
                                >() {

                            @Override
                            public void process(
                                    Integer sensorId,
                                    Context context,
                                    Iterable<Integer> totals,
                                    Collector<TrafficWindowResult> out
                            ) {

                                int totalVehicleCount =
                                        totals.iterator().next();

                                out.collect(
                                        new TrafficWindowResult(
                                                sensorId,
                                                context.window().getStart(),
                                                context.window().getEnd(),
                                                totalVehicleCount
                                        )
                                );
                            }
                        }
                );
        
        aggregatedStream.print();        

        env.execute("Traffic Analytics Job");
    }
}
