package com.firstgroupapp.aktutorial;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class producer1callwithkeys {
    public static void main (String[] args) throws ExecutionException, InterruptedException {
    	
    	final Logger logger = LoggerFactory.getLogger(producer1callwithkeys.class);
        System.out.print("Hello World");

        String bootstrapsServers="127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapsServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String,String> first_producer = new KafkaProducer<>(properties);
        
        for(int i=0; i<10;i++) {
        	System.out.print("===================================================================================================\n");
        	String topic = "my_first";
        	String value = "From producer1callwithkeys --->  " + Integer.toString(i);
        	String key = "id_" + Integer.toString(i);

        	//Create the Producer Record
            ProducerRecord<String,String> record = new ProducerRecord<>(topic, key, value);
            
            logger.info("key"+key);

            //Sending the data with Kafka Producer Callbacks with key
            first_producer.send(record, new Callback() {  
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {  
                    if (e== null) {  
                        logger.info("Successfully received the details as: \n" +  
                                "Topic:" + recordMetadata.topic() + "\n" +  
                                "Partition:" + recordMetadata.partition() + "\n" +  
                                "Offset" + recordMetadata.offset() + "\n" +  
                                "Timestamp" + recordMetadata.timestamp());  
                   }else {  
                        logger.error("Can't produce,getting error",e);  
                   }  
                }  
            }).get();  

        }
    	System.out.print("===================================================================================================\n");

        
        first_producer.flush();
        first_producer.close();
    }
}
