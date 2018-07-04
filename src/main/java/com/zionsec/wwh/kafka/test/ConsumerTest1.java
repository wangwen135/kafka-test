package com.zionsec.wwh.kafka.test;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class ConsumerTest1 {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.91:9092");
        props.put("group.id", "test1");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");

        props.put("auto.offset.reset", "earliest");
        // props.put("auto.offset.reset", "none");

        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("ftpuser"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
                JSONObject jsonObj = JSON.parseObject(json);
                String srcPort = jsonObj.getString("srcPort");
                String dstPort = jsonObj.getString("dstPort");
                if (srcPort.equals("20") || srcPort.equals("21") || dstPort.equals("20") || dstPort.equals("21")) {
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(),
                            record.value());
                }
            }
        }
    }
}
