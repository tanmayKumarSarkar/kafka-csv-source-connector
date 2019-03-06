package file.reader.Watcher;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.fasterxml.jackson.core.JsonProcessingException;

public class FileLooper {
	
	public void findFiles(String filePath, String processedFilePath, KafkaProducer kp, String producerTopic) {
		File file = new File(filePath);
		String fileDir = file.getAbsolutePath().substring(0,file.getAbsolutePath().lastIndexOf(File.separator));
		String fileNamePattern = file.getName();
		//System.out.println("fileDir : "+fileDir+ ", fileName : "+fileName);
		Collection<?> files = getAllFilesThatMatchFilenameExtension(fileDir, fileNamePattern);
	    files.forEach((tempFile) -> {
	    	try {
	    		FileToJSON fileToJSON = new FileToJSON();
				fileToJSON.fileJSONParser(tempFile.toString(), kp, producerTopic);
				String fileNameTemp = new File(tempFile.toString()).getName();
				Files.move(Paths.get(tempFile.toString()), Paths.get(processedFilePath + fileNameTemp), StandardCopyOption.REPLACE_EXISTING);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	       // System.out.println(temp);
	    });
	}
	
	public Collection getAllFilesThatMatchFilenameExtension(String directoryName, String extension) {
	    File directory = new File(directoryName);
	    return FileUtils.listFiles(directory, new WildcardFileFilter(extension), null);
	}	
}


