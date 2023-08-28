package com.mind2codes.com.demospringkafka.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("v1/demo")
@RequiredArgsConstructor
public class DemoController {

    KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping
    @PreAuthorize("hasRole('USER') or hasRole('ADMIN')")
    public ResponseEntity<String> getAll(@AuthenticationPrincipal Jwt jwt) {
        return new ResponseEntity<>("Hello world " + jwt.getSubject(), HttpStatus.OK);
    }

    @PostMapping("/produce")
    public ResponseEntity<String> produce(@RequestBody String message) throws ExecutionException, InterruptedException, TimeoutException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(initializeKafka());
        Future<RecordMetadata> metadataFuture = producer.send(
                new ProducerRecord<>("topic.demo.kafka", message)
        );
        producer.flush();
        metadataFuture.get(20, TimeUnit.SECONDS);

        return new ResponseEntity<>("Success", HttpStatus.CREATED);
    }

    private Properties initializeKafka() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        //properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        return properties;
    }
}
