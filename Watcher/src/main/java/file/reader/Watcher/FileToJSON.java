package file.reader.Watcher;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileToJSON {

	public String[] headers(String filePath)throws IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
	        return br.readLine().split(",");
	    }
	}
	
	public void fileJSONParser(String filePath, KafkaProducer kp, String producerTopic) throws JsonProcessingException, IOException {

    	ObjectMapper mapperObj = new ObjectMapper();
    	//String filePath = "F:\\Tanmay\\Programs\\kafka_2.12-2.1.1\\kafka_2.12-2.1.1\\test.csv";
    	String[] headers = headers(filePath);
    	//Stream<String> stream = Files.lines(Paths.get("F:\\Tanmay\\Programs\\kafka_2.12-2.1.1\\kafka_2.12-2.1.1\\test.csv"));
    	Stream<String> stream = Files.lines(Paths.get(filePath));
    	stream
    	.skip(1)
    	.map(l -> l.split(","))
    	.map(data -> IntStream.range(0, data.length)
                .boxed()
                .collect(Collectors.toMap(i -> headers[i], i -> data[i])))
                .forEach(l-> {
        			try {
	    				System.out.println(mapperObj.writeValueAsString(l));
	    				kp.send(new ProducerRecord(producerTopic, Integer.toString((int)Math.random()), mapperObj.writeValueAsString(l)));
	    			} catch (JsonProcessingException e) {
	    				e.printStackTrace();
	    			}
    		});
    }
}
