package org.example.parquet;

import org.apache.avro.Schema;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.model.Message;
import org.example.utils.JsonParser;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



public class WriteJavaObjectToParquetFile {
    SparkSession sparkSession;
    long[] startTime = new long[11];
    long limit = 864_00000;
    int batchSize=1000;


    ArrayList<ArrayList<Message>> batches = new ArrayList<>();

   public WriteJavaObjectToParquetFile() {
        for (int i = 0; i < 11; i++) {
            startTime[i]=-1;
            batches.add(new ArrayList<Message>());
        }
        sparkSession = SparkSession.builder().appName("Write to Parquet").master("local[*]").getOrCreate();
    }
    public void writeRecord(Message message){
        int stationID = (int) message.getStation_id();
        batches.get(stationID).add(message);
        if(batches.get(stationID).size()==batchSize){
          List<Message> messages = batches.get(stationID);
            Dataset<Row> df = sparkSession
                    .createDataFrame(messages, Message.class);
            df.coalesce(1).write().format("parquet").mode("append").save("WeatherStationStatusMessages"+File.separator+stationID+"s"+startTime[stationID]);
            batches.get(stationID).clear();
        }else if (batches.get(stationID).size()==1){
            startTime[stationID]=message.getStatus_timestamp();
        }else if(message.getStatus_timestamp()>startTime[stationID]+limit){
            List<Message> messages = batches.get(stationID);
            messages.remove(messages.size()-1);
            Dataset<Row> df = sparkSession
                    .createDataFrame(messages, Message.class);
            df.coalesce(1).write().format("parquet").mode("append").save("WeatherStationStatusMessages"+File.separator+stationID+"s"+startTime[stationID]);
            batches.get(stationID).clear();
            batches.get(stationID).add(message);
            startTime[stationID]=message.getStatus_timestamp();

        }

    }
   /* public void writeRecord(Message message) {

        int stationID = (int) message.getStation_id();
        if (writers.get(stationID) == null) {
            try {
                writers.set(stationID,  createWriter("parquetFiles"+File.separator+stationID+"s"+ message.getStatus_timestamp()));
                startTime[stationID]=message.getStatus_timestamp();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else if(message.getStatus_timestamp()>startTime[(int)message.getStation_id()]+limit){
            writeBatch(stationID);
            batches.get((int)message.getStation_id()).clear();
            try {
                writers.set(stationID,  createWriter("parquetFiles"+File.separator+stationID+"s"+ message.getStatus_timestamp()));
                startTime[stationID]=message.getStatus_timestamp();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        batches.get((int) message.getStation_id()).add(message);

        if (batches.get((int) message.getStation_id()).size() == 10) {
           writeBatch(stationID);
           batches.get((int)message.getStation_id()).clear();
        }

    }*/

  /*  public static ParquetWriter<Message> createWriter(String path,Message message) throws IOException {
        Schema schema;

             schema = ReflectData.AllowNull.get().getSchema(Message.class);
        Path dataFile = new Path(path);
        ParquetWriter<Message> writer = AvroParquetWriter.<Message>builder(dataFile)
                .withSchema(schema)
                .withDataModel(ReflectData.get())
                .withConf(new Configuration())
                .withCompressionCodec(SNAPPY)
                .withWriteMode(OVERWRITE)
                .build();
        writer.write(message);
        return writer;

    }*/

    public static void main(String[] args) throws Exception {
        // Create a schema for the Java object.
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File("parquet_schema"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        WriteJavaObjectToParquetFile writeJavaObjectToParquetFile = new WriteJavaObjectToParquetFile();

    }
}