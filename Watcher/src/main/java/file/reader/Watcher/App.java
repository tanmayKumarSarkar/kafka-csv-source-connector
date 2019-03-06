package file.reader.Watcher;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field.Str;

import com.fasterxml.jackson.core.JsonProcessingException;

public class App {
    public static void main( String[] args ) throws JsonProcessingException, IOException {
    	
    	String filePath = args[0];			//File path with Wild Card Pattern File Name
    	String bootstrapServer = args[1];	//"localhost:9092"; Kafka server host:port
    	String producerTopic = args[2];		//"myexample"; kafka producer topic name
    	String processedFolderName = "processed";
    	
    	File file = new File(filePath);
		String fileDir = file.getAbsolutePath().substring(0,file.getAbsolutePath().lastIndexOf(File.separator));
		String processedFilePath = fileDir+ "\\"+processedFolderName+"\\";
		
		//make processed Dir if not exists
		File dir = new File(processedFilePath);
	    if (!dir.exists()) dir.mkdirs();
		
    	FileLooper fl = new FileLooper();
    	
    	Producer producer = new Producer();
    	KafkaProducer kp = producer.createProducer(bootstrapServer);
    	//kp.send(new ProducerRecord("myexample", Integer.toString(1), "test message - " + 1 ));
    	fl.findFiles(filePath, processedFilePath, kp, producerTopic);
	    kp.close();
	    
	    try {
            WatchService watcher = FileSystems.getDefault().newWatchService();
            Path dir2 = Paths.get(fileDir);
            dir2.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            System.out.println("Watch Service registered for Directory: " + dir2.getFileName());
            while (true) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                    return;
                }
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path fileName = ev.context();
                    System.out.println(kind.name() + ": " + fileName);
                    if(kind.name() == "ENTRY_CREATE") { /*( && fileName != "processed")*/

                    	KafkaProducer kp2 = producer.createProducer(bootstrapServer);
                    	fl.findFiles(filePath, processedFilePath, kp2, producerTopic);
                	    kp2.close();
                	    
                    }
                }
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (IOException ex) {
            System.err.println(ex);
        }
    	
//    	Producer producer = new Producer();
//    	KafkaProducer kp = producer.createProducer(bootstrapServer);
//    	kp.send(new ProducerRecord("myexample", Integer.toString(1), "test message - " + 1 ));
//    	kp.close();
    
    }
    
}
