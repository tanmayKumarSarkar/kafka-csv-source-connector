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
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


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
	    				String str = ObjectToJSONString(mapperObj, l);
	    				kp.send(new ProducerRecord(producerTopic, Integer.toString((int)Math.random()), str));
	    			} catch (JsonProcessingException e) {
	    				e.printStackTrace();
	    			}
    		});
    }
	
	public String ObjectToJSONString(ObjectMapper mapperObj, Object Obj) throws JsonProcessingException {
		String JSONStr = null;
		
		JSONStr = mapperObj.writeValueAsString(Obj);
		Gson gson = new Gson();
		JsonObject body = gson.fromJson(JSONStr, JsonObject.class);
		body.keySet().forEach(key ->
	    {
//	        Object keyvalue = body.get(key);
	    	if(checkNumberic(body.get(key).toString())) {
	    		try {
	    			Integer num = Integer.parseInt(body.get(key).toString().trim().replaceAll("\"", ""));
        			body.addProperty(key, num);
	                
	            } catch (NumberFormatException e1) {
	            	try {
	            		Long num = Long.parseLong(body.get(key).toString().trim().replaceAll("\"", ""));
	            		body.addProperty(key, num);
	            	}catch (NumberFormatException e2) {
	            		try {
	            			Double num = Double.parseDouble(body.get(key).toString().trim().replaceAll("\"", ""));
	    	                body.addProperty(key, num);
	                	}catch (NumberFormatException e3) {
	                		
	                    }
	                }
	            } 
//	    		body.addProperty(key, body.get(key).toString().replaceAll("\"", ""));
	    	}
	        //System.out.println(checkNumberic(keyvalue.toString()));
//	        if(checkNumberic(keyvalue.toString())) {
//	        	try {
//	                Double num = Double.parseDouble(keyvalue.toString().trim());
//	                body.addProperty(key, num);
//	            } catch (NumberFormatException e1) {
//	            	try {
//	            		Long num = Long.parseLong(keyvalue.toString().trim());
//	            		 body.addProperty(key, num);
//	            	}catch (NumberFormatException e2) {
//	                	Integer num = Integer.parseInt(keyvalue.toString().trim());
//	                	 body.addProperty(key, num);
//	                }
//	            } 
//	        	
//	        }
	        //System.out.println("key: "+ key + " value: " + keyvalue);
	        //for nested objects iteration if required
	        //if (keyvalue instanceof JSONObject)
	        //    printJsonObject((JSONObject)keyvalue);
	    });
//		JsonArray results = body.get("results").getAsJsonArray();
//		JsonObject firstResult = results.get(0).getAsJsonObject();
//		JsonElement address = firstResult.get("formatted_address");
//		System.out.println(body + " : "+ results+ " : "+ firstResult +" : "+ address.getAsString());
		System.out.println(body);
		//System.out.println(JSONStr);
		return body.toString();
	}
	
	public boolean checkNumberic (String string) {
		string = string.replaceAll("\"", "");
		boolean numeric = false;
		//System.out.println(string);
		try {
            Double num = Double.parseDouble(string);
            numeric = true;
        } catch (NumberFormatException e1) {
        	try {
        		Long num = Long.parseLong(string);
        		numeric = true;
        	}catch (NumberFormatException e2) {
        		try {
        			Integer num = Integer.parseInt(string);
                	numeric = true;
            	}catch (NumberFormatException e3) {
                	numeric = false;
                }
            }
        } 
        return numeric;
	}
	
//	public JsonObject parseNumber (String string) {
//
//		try {
//            Double num = Double.parseDouble(string);
//            return num;
//        } catch (NumberFormatException e1) {
//        	try {
//        		Long num = Long.parseLong(string);
//        		return num;
//        	}catch (NumberFormatException e2) {
//            	Integer num = Integer.parseInt(string);
//            	return num;
//            }
//        } 
//	}
	
//	public String schema(final Class<?> clazz) {
//		  ServiceDefinition sd = build(clazz);
//		  Gson gson = new Gson();
//		  return gson.toJson(sd);
//		}
}
