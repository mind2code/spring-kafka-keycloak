package com.mind2codes.com.demospringkafka.common.securite;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "topic.demo.kafka", groupId = "group_id")
    public void consume(String message) {
        log.info("Kafka message -> ", message);
    }
}
