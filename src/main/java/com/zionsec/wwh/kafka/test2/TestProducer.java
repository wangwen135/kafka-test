package com.zionsec.wwh.kafka.test2;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

public class TestProducer {
    public static void main(String[] args) {
        // long events = Long.parseLong(args[0]);
        long events = 100;
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.214:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        String topic = "page_view";
        
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        
        //查看话题分区信息
        List<PartitionInfo> partList=  producer.partitionsFor(topic);
        
        for (PartitionInfo partitionInfo : partList) {
            System.out.println(partitionInfo);
        }
        
        
        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = nEvents +"_"+ runtime + ",www.example.com," + ip;
            
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, ip, msg);
            Future<RecordMetadata> f = producer.send(data);
            
        }
        producer.close();
    }
}
