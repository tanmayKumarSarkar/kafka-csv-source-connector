package file.reader.Watcher;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
	
    public KafkaProducer createProducer(String bootstrapServer){
    	Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServer);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        return kafkaProducer;
//        try{
//            for(int i = 0; i < 10; i++){
//                System.out.println(i);
//                kafkaProducer.send(new ProducerRecord("myexample", Integer.toString(i), "test message - " + i ));
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }finally {
//            kafkaProducer.close();
//        }
    }
}