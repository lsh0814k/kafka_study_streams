package com.example.streams;

import com.example.streams.util.JsonUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class StreamListener {
    private static final String inputTopic = "checkout.complete.v1";
    private static final String outputTopic = "checkout.productId.aggregated.v1";
    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {
        KStream<String, String> inputStreams = builder.stream(inputTopic);
        inputStreams
                .map((k, v) -> new KeyValue<>(JsonUtils.getProductId(v), JsonUtils.getAmount(v)))
                // group by productId
                .groupByKey(Grouped.with(Serdes.Long(), Serdes.Long()))
                // window 설정
                .windowedBy(TimeWindows.of(Duration.ofMillis(1)))
                // apply sum method
                .reduce(Long::sum)
                // map the window key
                .toStream((k,v) -> k.key())
                // outputTopic 에 보낼 Json String 으로 Generate
                .mapValues(JsonUtils::getSendingJson)
                // outputTopic 으로 보낼 key 값을 null 설정
                .selectKey((k, v) -> null)
                // outputTopic 으로 메세지 (null, jsonString) 전송 설정
                .to(outputTopic, Produced.with(null, Serdes.String()));

        return inputStreams;
    }
}
