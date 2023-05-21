package org.example.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.example.model.Message;
import org.example.utils.JsonParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;

public class WriteJavaObjectToParquetFile {
    long[] startTime = new long[10];
    long limit = 864_00000;
    ParquetWriter<GenericRecord>[] writers = new ParquetWriter[10];
    ArrayList<ArrayList<GenericRecord>> batches = new ArrayList<>();

    WriteJavaObjectToParquetFile() {
        for (int i = 0; i < 10; i++) {
            startTime[i]=-1;
            batches.add(new ArrayList<GenericRecord>());
        }
    }

    public void writeRecord(Message message) {
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File("parquet_schema"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Create a GenericRecord to hold the data for the Java object.
        GenericRecord record = new GenericData.Record(schema);
        record.put("station_id", message.getStation_id());
        record.put("s_no", message.getS_no());
        record.put("battery_status", message.getBattery_status());
        record.put("status_timestamp", message.getStatus_timestamp());
        record.put("weather", new GenericData.Record(schema.getField("weather").schema()));
        GenericRecord record1 = (GenericRecord) record.get("weather");
        record1.put("humidity", message.getWeather().getHumidity());
        record1.put("temperature", message.getWeather().getTemperature());
        record1.put("wind_speed", message.getWeather().getWind_speed());
        int stationID = (int) message.getStation_id();
        if (writers[stationID] == null) {
            try {
                writers[stationID] = createWriter("parquetFiles"+File.separator+stationID+ message.getStatus_timestamp());
                startTime[stationID]=message.getStatus_timestamp();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else if(message.getStatus_timestamp()>startTime[(int)message.getStation_id()]+limit){
            writeBatch(stationID);
            batches.get((int)message.getStation_id()).clear();
            try {
                writers[stationID]=createWriter("parquetFiles"+File.separator+stationID+"s"+message.getStatus_timestamp());
                startTime[stationID]=message.getStatus_timestamp();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        batches.get((int) message.getStation_id()).add(record);

        if (batches.get((int) message.getStation_id()).size() == 10) {
           writeBatch(stationID);
           batches.get((int)message.getStation_id()).clear();
        }

    }
    public void writeBatch(int stationId){
        for(GenericRecord genericRecord : batches.get(stationId)){
            try {
                writers[stationId].write(genericRecord);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public ParquetWriter<GenericRecord> createWriter(String path) throws IOException {
        Schema schema;
        try {
            schema = new Schema.Parser().parse(new File("parquet_schema"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Path dataFile = new Path(path);
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(dataFile)
                .withSchema(schema)
                .withDataModel(ReflectData.get())
                .withConf(new Configuration())
                .withCompressionCodec(SNAPPY)
                .withWriteMode(OVERWRITE)
                .build();

        return writer;

    }

    public static void main(String[] args) throws Exception {
        // Create a schema for the Java object.
        Message message = JsonParser.convertStringToMessage("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Low\",\n" +
                "\"status_timestamp\": 1684534702959,\n" +
                "\"weather\": { \"humidity\": 24,\n" +
                "\"temperature\": 85,\n" +
                "\"wind_speed\": 8\n" +
                "}\n" +
                "}");
        WriteJavaObjectToParquetFile writeJavaObjectToParquetFile = new WriteJavaObjectToParquetFile();
        writeJavaObjectToParquetFile.writeRecord(message);
        writeJavaObjectToParquetFile.writeRecord(message);
        writeJavaObjectToParquetFile.writeRecord(message);
        writeJavaObjectToParquetFile.writeRecord(message);
        writeJavaObjectToParquetFile.writeRecord(message);
        writeJavaObjectToParquetFile.writeRecord(message);
        writeJavaObjectToParquetFile.writeRecord(message);
        writeJavaObjectToParquetFile.writeRecord(message);
        writeJavaObjectToParquetFile.writeRecord(message);
    }
}