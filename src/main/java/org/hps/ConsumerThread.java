package org.hps;



import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ConsumerThread implements Runnable {
    private static final Logger log = LogManager.getLogger(KafkaConsumerTestAssignor.class);
    public static KafkaConsumer<String, Customer> consumer = null;
    static float maxConsumptionRatePerConsumer = 100.0f;
    static Double maxConsumptionRatePerConsumer1 = 100.0d;



    @Override
    public void run() {
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
        int receivedMsgs = 0;
        // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LagBasedPartitionAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));
        log.info("Subscribed to topic {}", config.getTopic());

        Long sleep;

        while (receivedMsgs < config.getMessageCount()) {
            //  consumer.enforceRebalance();
            ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            //long start = System.currentTimeMillis();
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
                }
            }

            try {
                Thread.sleep(Long.parseLong(config.getSleep()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            if (commit) {
                consumer.commitSync();
            }


        }
        log.info("Received {} messages", receivedMsgs);
    }
}
