package main;

import java.io.File;
import java.io.IOException;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * @author HPPranaa
 */

public class SendImage {

    Producer<String, byte[]> createProducer;
    Path path;
    Config config;

    public SendImage(Config config) {
        this.config = config;

        // Get the Kafka settings ready and assign them to a producer Object
        Properties props = new Properties();
        props.put("bootstrap.servers",config.kafkaEndpoint);
        props.put("acks", "1");
        props.put("compression.type","snappy");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        createProducer = new KafkaProducer<>(props);

        path=FileSystems.getDefault().getPath(config.incomingDirectory);


    }

    // Split file being sent in pieces of 10kB
    private ArrayList splitFile(String file, byte[] datum) {
        int l = datum.length;
        int block = 10240;
        int numblocks=l/block;
        int counter=0, totalSize=0;
        int marker=0;
        byte[] chunk;
        ArrayList<byte[]> data = new ArrayList<>();
        for(int i = 0; i < numblocks; i++ ) {
            counter++;
            chunk=Arrays.copyOfRange(datum, marker, marker+block);
            data.add(chunk);
            totalSize+=chunk.length;
            marker+=block;
        }
        chunk=Arrays.copyOfRange(datum,marker,l);
        data.add(chunk);
        // the null value is a flag to the consumer, specifiying that it has reached the end of the file
        data.add(null);

        return data;
    }

    public void start()throws IOException, InterruptedException {
        String fileName;
        byte[] fileData;
        ArrayList<byte[]> allChunks;
        // Watcher watches for file system changes
        WatchService watcher = FileSystems.getDefault().newWatchService();
        WatchKey key;
        path.register(watcher, ENTRY_CREATE);

        while(true) {
            key = watcher.take();
            // Code beyond this point is executed on a change in filesystem

            for(WatchEvent<?> event: key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                // Only if a file is added is any action taken
                if(kind == ENTRY_CREATE) {
                    WatchEvent<Path> ev = (WatchEvent<Path>)event;
                    Path filename = ev.context();
                    fileName = filename.toString();
                    // Give time to ensure full read
                    Thread.sleep(500);
                    fileData=Files.readAllBytes(FileSystems.getDefault().getPath(config.incomingDirectory+File.separator+fileName));
                    allChunks=splitFile(fileName,fileData);
                    for (int i=0;i < allChunks.size();i++)
                        publishMessage(fileName, (allChunks.get(i)));
                    System.out.println("Published file "+fileName);



                }
            }
            key.reset();
        }
    }

    public void publishMessage(String key, byte[] bytes) {
        ProducerRecord <String, byte[]> data =new ProducerRecord<>(config.topic, key, bytes);
        createProducer.send(data);

    }

    public static void main(String args[]) throws Exception {

        ConsulConfig cc = new ConsulConfig();
        Config config = new Config(cc);
        SendImage sImage = new SendImage(config);
        try {
            sImage.start();
        } catch(InterruptedException e) {
            Logger.getLogger(SendImage.class.getName()).log(Level.SEVERE, null, e);
        }

    }

}
