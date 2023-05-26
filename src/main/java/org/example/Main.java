package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.bitcask.BitCaskImpl;
import org.example.bitcask.CustomizedThread;
import org.example.parquet.WriteJavaObjectToParquetFile;
import org.example.utils.JsonParser;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Main {
    static String nameOfDirectory = "BitcaskStore";


    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "afdsdgf");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        final Consumer<Integer, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList("test21"));
        BitCaskImpl bitCask = new BitCaskImpl();
        bitCask.open(nameOfDirectory);
        WriteJavaObjectToParquetFile writeJavaObjectToParquetFile = new WriteJavaObjectToParquetFile();
        CustomizedThread customizedThread = new CustomizedThread(bitCask);
        boolean merged = false;
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<Integer, String> record : records) {
                bitCask.put(record.value());
                if (bitCask.currentIDOfActiveFile > 10 && !merged) {
                    bitCask.createReplicas();
                    customizedThread.start();
                    merged = true;
                }
                if (merged && !customizedThread.isAlive()) {
                    bitCask.renameFiles();
                    customizedThread= new CustomizedThread(bitCask);
                    merged = false;
                }
                System.out.println("from central service");
                System.out.println(record.value());
                writeJavaObjectToParquetFile.writeRecord(JsonParser.convertStringToMessage(record.value()));
            }
        }


    }
}