package main;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.network.Receive;

/**
 * @author HPPranaa
 */

public class ReceiveImage {

    Properties props;
    KafkaConsumer<String,byte[]> createConsumer;
    Config config;

    public ReceiveImage(Config config) {
        this.config = config;

        // Set properies for kafka
        props = new Properties();
        props.put("bootstrap.servers", config.kafkaEndpoint);
        props.put("group.id", config.groupID);
        props.put("enable.auto.commit", "true");
        props.put("compression.type","snappy");
        props.put("fetch.message.max.bytes","7340032");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

    public void consCreate() {
        createConsumer = new KafkaConsumer<String, byte[]>(props);
        createConsumer.subscribe(Arrays.asList(config.topic));
    }

    public void start()throws IOException {
        String name = null;
        ByteArrayOutputStream bas = new ByteArrayOutputStream();

        // Poll endlessly all the while poll for records from kafka
        while(true) {
            ConsumerRecords<String,byte[]> records = createConsumer.poll(100);
            for(ConsumerRecord<String,byte[]> record : records) {

                // Check for end of file
                if(record.value() == null) {

                    // Reached EOF
                    System.out.println("Writing file: " + name);
                    writeFile(name,bas.toByteArray());
                    bas.reset();
                }
                else {

                    // Another chunk of file, add to queue
                    name = record.key();
                    bas.write(record.value());
                }
            }
        }
    }

    public void writeFile(String name, byte[] rawdata)throws IOException {
        File file = new File(config.outgoingDirectory);
        if(!file.exists())
            file.mkdirs();
        FileOutputStream fos = new FileOutputStream(config.outgoingDirectory+File.separator+name);
        fos.write(rawdata);
        fos.flush();
        fos.close();
    }



    public static void main(String args[])throws Exception {
        ConsulConfig cc = new ConsulConfig();
        Config config = new Config(cc);
        ReceiveImage rImage = new ReceiveImage(config);
        try {
            rImage.consCreate();
            rImage.start();
        } catch(IOException e) {
            Logger.getLogger(ReceiveImage.class.getName()).log(Level.SEVERE, null, e);
        }

    }
}
