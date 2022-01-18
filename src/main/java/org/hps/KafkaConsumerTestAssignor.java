package org.hps;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerTestAssignor {
    private static final Logger log = LogManager.getLogger(KafkaConsumerTestAssignor.class);
    public static KafkaConsumer<String, Customer> consumer = null;
    static double maxConsumptionRatePerConsumer = 100.0;

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
        int receivedMsgs = 0;
        // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LagBasedPartitionAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        Integer eventsPerSeconds = Integer.parseInt(System.getenv("Events_Per_SEC"));
        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));
        log.info("Subscribed to topic {}", config.getTopic());

        while (receivedMsgs < config.getMessageCount()) {
            ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            Instant start = Instant.now();
            for (ConsumerRecord<String, Customer> record : records) {
                log.info("Received message:");
                log.info("\tpartition: {}", record.partition());
                log.info("\toffset: {}", record.offset());
                log.info("\tvalue: {}", record.value().toString());
                if (record.headers() != null) {
                    log.info("\theaders: ");
                    for (Header header : record.headers()) {
                        log.info("\t\tkey: {}, value: {}", header.key(), new String(header.value()));
                    }
                    Thread.sleep(1000 / eventsPerSeconds);
                    log.info("Event waiting time = {} ms = {} sec",(System.currentTimeMillis() - record.timestamp())
                    , (System.currentTimeMillis() - record.timestamp())/1000.0d);
                }
            }
            Instant end = Instant.now();
            long resSec = Duration.between(start, end).toSeconds();
            float consRatePerSec= (float)records.count()/(float)(resSec);

            log.info("total time to process {} events is {} seconds", records.count(), resSec);
            log.info("Consumption rate per sec {}", consRatePerSec);

            if (commit) {
                consumer.commitSync();
            }
        }
        log.info("Received {} messages", receivedMsgs);
    }

}




