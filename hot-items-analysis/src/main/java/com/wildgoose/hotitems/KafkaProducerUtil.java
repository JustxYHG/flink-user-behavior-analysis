package com.wildgoose.hotitems;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * @Description : 定义一个 Kafka 生产者，往 hot_items 主题中实时写入数据
 * @Author : JustxzzZ
 * @Date : 2023.05.02 11:11
 */
public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception {

        writeToKafka("hot_items");

    }

    public static void writeToKafka(String topic) throws Exception {

        // Kafka 配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义一个 Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 用缓冲方式读取文件
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader("hot-items-analysis/src/main/resources/UserBehavior.csv"));

        String line;

        while ((line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            // 用 producer 发送数据
            kafkaProducer.send(producerRecord);
        }

        kafkaProducer.close();

    }
}
