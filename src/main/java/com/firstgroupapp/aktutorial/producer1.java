package com.firstgroupapp.aktutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producer1 {
    public static void main (String[] args){
        System.out.print("Hello World");

        String bootstrapsServers="127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapsServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String,String> first_producer = new KafkaProducer<>(properties);
        //Create the Producer Record
        ProducerRecord<String,String> record = new ProducerRecord<>("my_first","Hey Kafka");
        //Sending the data
        first_producer.send(record);
        first_producer.flush();
        first_producer.close();

    }
}
