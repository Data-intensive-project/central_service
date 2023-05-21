package org.example.bitcask;

import org.example.model.Message;
import org.example.utils.JsonParser;
import org.example.utils.Readers;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class BitCaskImpl implements BitCask {
    HashMap<String, KeyDirectoryRecord> keyDirectory;
    String activeFileName;
    String directoryName;
    long currentIDOfActiveFile;

    public BitCaskImpl() {
        keyDirectory = new HashMap<>();
    }

    @Override
    public void open(String directoryName) {
        boolean created = new File(directoryName).mkdirs();
        this.directoryName = directoryName;
        if (created) {
            File myObj = new File(directoryName + File.separator + "current1");
            try {
                myObj.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            currentIDOfActiveFile = 1;
            activeFileName = "current1";
            System.out.println("created Successfully");
        } else {
            File dir = new File(directoryName);
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if (child.getName().toString().contains("current")) {
                        activeFileName = child.getName();
                        currentIDOfActiveFile = Long.parseLong(child.getName().substring(7));
                    }
                }
            }
            System.out.println("already exist");
        }
    }

    @Override
    public Message get(String key) {
        if (!keyDirectory.containsKey(key)) {
            System.out.println("not found");
            return null;
        }
        KeyDirectoryRecord keyDirectoryRecord = keyDirectory.get(key);
        File fileToReadFrom;
        if (currentIDOfActiveFile == keyDirectoryRecord.fileId) {
            fileToReadFrom = new File(directoryName + File.separator + "current" + currentIDOfActiveFile);
        } else {
            fileToReadFrom = new File(directoryName + File.separator + (keyDirectoryRecord.fileId == 0?"hint":keyDirectoryRecord.fileId));
        }
        byte[] data;
        try {
            System.out.println("file has size : " + fileToReadFrom.getName()+" "+ fileToReadFrom.length());
            data = readFromFile(fileToReadFrom.getAbsolutePath(), keyDirectoryRecord.valuePos, keyDirectoryRecord.valueSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            if (keyDirectoryRecord.fileId==0){
                return Message.createFromByteArray(data);
            }
            return MessageSaved.createFromByteArray(data).value;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String value) {
        Message message = JsonParser.convertStringToMessage(value);
        String key = String.valueOf(message.getStation_id());
        System.out.println(key);
        File file = new File(directoryName + File.separator + activeFileName);
        if (file.length() + value.getBytes().length > 2000) {
            renameAndCreateNewActiveFile();
        }
        byte[] data = prepareDataForActiveFile(message, key);
        writeDateInFile(directoryName + File.separator + activeFileName, data);
    }

    @Override
    public void merge(String directoryName) {
        File dir = new File(directoryName);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null && directoryListing.length <= 1) {
            System.out.println("need more files to merge");
            return;
        }
        createReplicas();
        HashSet<String> updated = new HashSet<>();
        ArrayList<MessageSaved> latest = new ArrayList<>();
        if (directoryListing != null) {
            for (long i = currentIDOfActiveFile - 1; i >= 1; i--) {
                String filename = String.valueOf(i);
                String filepath = directoryName + File.separator + filename+"_copy";
                ArrayList<MessageSaved> result = scrapFiles(updated, filepath);
                latest.addAll(result);
                if (updated.size() == keyDirectory.size()) {
                    break;
                }
            }
            if (updated.size() != keyDirectory.size()) {
                File hintfile = new File(directoryName + File.separator + "hint");
                if (hintfile.exists()) {
                    String filePath = directoryName + File.separator + "hint_copy";
                    ArrayList<MessageSaved> result = scrapHintFile(updated);
                    latest.addAll(result);
                }
            }
            createHintFile(latest);
        }
    }
    public void rehash(){
        //TODO read all hint record from the hint file an populate the KeyDirectory
        String path = directoryName+File.separator+"hint_index";
        ArrayList<HintRecord> hintRecords;
        try {
            hintRecords = Readers.ReadAllHintRecord(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        for (HintRecord hintRecord : hintRecords){
            keyDirectory.put(hintRecord.key,hintRecord.keyDirectoryRecord);
        }
    }

    private ArrayList<MessageSaved> scrapHintFile(HashSet<String> updated) {
        File hintIndexFile = new File(directoryName + File.separator + "hint_index_copy");
        ArrayList<HintRecord> keyDirectoryRecords = null;
        try {
            keyDirectoryRecords = Readers.ReadAllHintRecord(hintIndexFile.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        ArrayList<MessageSaved> result = new ArrayList<>();
        for (HintRecord hintRecord : keyDirectoryRecords
        ) {
            try {
                byte[] data = readFromFile(directoryName+File.separator+"hint_copy", hintRecord.keyDirectoryRecord.valuePos, hintRecord.keyDirectoryRecord.valueSize);
                Message message = Message.createFromByteArray(data);
                MessageSaved messageSaved = new MessageSaved(hintRecord.keyDirectoryRecord, message, String.valueOf(message.getStation_id()));
                result.add(messageSaved);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    private ArrayList<MessageSaved> scrapFiles(HashSet<String> updated, String filePath) {

        ArrayList<MessageSaved> messages = new ArrayList<>();
        ArrayList<MessageSaved> messagesSaved = null;
        try {
            messagesSaved = Readers.ReadAllMessageSavedObjects(filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (MessageSaved messageSaved : messagesSaved) {
            if (updated.contains(String.valueOf(messageSaved.value.getStation_id()))) {
                continue;
            }
            messages.add(messageSaved);
            updated.add(String.valueOf(messageSaved.value.getStation_id()));
        }

        return messages;
    }

    public void createHintFile(ArrayList<MessageSaved> messagesSaved) {
        String hintFilePath = directoryName + File.separator + "hint_copy";
        String hint_index_file = directoryName + File.separator+"hint_index_copy";

        File hintFile = new File(hintFilePath);
        hintFile.delete();
        File indexFile = new File(hint_index_file);
        indexFile.delete();
        try {
            hintFile.createNewFile();
            indexFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (MessageSaved messageSaved : messagesSaved) {
            messageSaved.keyDirectoryRecord.fileId = 0;
            File hintFileTemp = new File(hintFilePath);
            messageSaved.keyDirectoryRecord.valuePos = hintFileTemp.length();
            messageSaved.keyDirectoryRecord.valueSize = messageSaved.value.toByteArray().length;
            if (! (keyDirectory.get(messageSaved.key).fileId==currentIDOfActiveFile)){
                keyDirectory.put(messageSaved.key, messageSaved.keyDirectoryRecord);
            }
            writeDateInFile(hintFilePath, messageSaved.value.toByteArray());
            HintRecord hintRecord = new HintRecord(messageSaved.key,messageSaved.keyDirectoryRecord);
            try {
                writeDateInFile(hint_index_file,intToByteArray(hintRecord.toByteArray().length));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            writeDateInFile(hint_index_file,hintRecord.toByteArray());
            System.out.println("hint index file size : "+hintFileTemp.length());
        }
        try {
            ArrayList<HintRecord>rr = Readers.ReadAllHintRecord(hint_index_file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            removeOldFiles();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        renameFiles();
    }

    public void createReplicas() {
        try {
          if(testIfHintExist()){
              Files.copy(Paths.get(directoryName+File.separator+"hint"),Paths.get(directoryName+File.separator+"hint_copy"),StandardCopyOption.REPLACE_EXISTING);
              Files.copy(Paths.get(directoryName+File.separator+"hint_index"),Paths.get(directoryName+File.separator+"hint_index_copy"),StandardCopyOption.REPLACE_EXISTING);
          }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (long i = currentIDOfActiveFile-1;i>=1;i--){
            File original = new File(directoryName+File.separator+i);
            File file = new File(directoryName+File.separator+i+"_copy");
            try {
                Files.copy(Paths.get(original.getPath()),Paths.get(file.getPath()), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
    private boolean testIfHintExist() throws IOException {
        return new File(directoryName+File.separator+"hint").exists();
    }
    public void removeOldFiles() throws IOException {
       deleteHintFiles();
       deleteOldDataFiles();
    }
    private void deleteHintFiles(){
        File file  = new File(directoryName+File.separator+"hint");
        File file1 = new File(directoryName+File.separator+"hint_index");
        file.delete();
        file1.delete();
    }
    private void deleteOldDataFiles(){
        long start = currentIDOfActiveFile-1;
        for (long  i =start;i>=1;i--){
            File file = new File(directoryName+File.separator+i);
            File fileCopy = new File(directoryName+File.separator+i+"_copy");
            file.delete();
            fileCopy.delete();
        }
    }
    public void renameFiles(){
      renameHintFiles();
      renameActiveFile();
      updateKeyDir();
    }
    private void renameHintFiles(){
        File hint = new File(directoryName+File.separator+"hint_copy");
        File hintindex = new File(directoryName+File.separator+"hint_index_copy");
        File hint2 = new File(directoryName+File.separator+"hint");
        File hintindex2 = new File(directoryName+File.separator+"hint_index");
        hint.renameTo(hint2);
        hintindex.renameTo(hintindex2);
    }
    private void renameActiveFile(){
        File file = new File(directoryName+File.separator+activeFileName);
        File newFile  = new File(directoryName+File.separator+"current1");
        file.renameTo(newFile);

    }
    private void updateKeyDir(){
        ArrayList<KeyDirectoryRecord>records = new ArrayList<>();
        for (Map.Entry<String,KeyDirectoryRecord> entry : keyDirectory.entrySet()){
            if(entry.getValue().fileId == currentIDOfActiveFile){
                KeyDirectoryRecord keyDirectoryRecord = entry.getValue();
                keyDirectoryRecord.fileId=1;
                keyDirectory.put(entry.getKey(),keyDirectoryRecord );
            }
        }
        currentIDOfActiveFile=1;
        activeFileName="current"+currentIDOfActiveFile;
    }

    public void renameAndCreateNewActiveFile() {
        File file = new File(directoryName + File.separator + activeFileName);
        File rename = new File(directoryName + File.separator + activeFileName.substring(7));
        file.renameTo(rename);
        currentIDOfActiveFile++;
        activeFileName = "current" + currentIDOfActiveFile;
        File newFile = new File(directoryName + File.separator + activeFileName);
        try {
            newFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] prepareDataForActiveFile(Message message, String key)  {
        File fileToWriteIn = new File(directoryName + File.separator + activeFileName);
        KeyDirectoryRecord keyDirectoryRecord = new KeyDirectoryRecord();
        keyDirectoryRecord.fileId = currentIDOfActiveFile;
        keyDirectoryRecord.timeStamp = message.getStatus_timestamp();
        keyDirectoryRecord.valuePos = fileToWriteIn.length()+4;
        MessageSaved messageSaved = new MessageSaved(keyDirectoryRecord, message, key);
        keyDirectoryRecord.valueSize = messageSaved.toByteArray().length;
        messageSaved.keyDirectoryRecord.valueSize = keyDirectoryRecord.valueSize;
        keyDirectory.put(key, keyDirectoryRecord);
        try {
            writeDateInFile(fileToWriteIn.getPath(),intToByteArray((int)keyDirectoryRecord.valueSize));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return messageSaved.toByteArray();
    }

    public void writeDateInFile(String filepath, byte[] data) {
        File fileToWriteIn = new File(filepath);
        try {
            Files.write(Paths.get(fileToWriteIn.getPath()), data, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private byte[] intToByteArray ( final int i ) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(i);
        dos.flush();
        return bos.toByteArray();
    }
    private int convertByteArrayToInt(byte[] intBytes){
        ByteBuffer byteBuffer = ByteBuffer.wrap(intBytes);
        return byteBuffer.getInt();
    }
    private byte[] readFromFile(String filePath, long position, int size)
            throws IOException {
        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        file.seek(position);
        byte[] bytes = new byte[size];
        file.read(bytes);
        file.close();
        return bytes;
    }

    public static void main(String[] args) throws IOException {

         BitCaskImpl bitCask = new BitCaskImpl();
        bitCask.open("testFolder");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683744444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683444444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 122,\n" +
                "\"wind_speed\": 10\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 7,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 2,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Mxdium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        System.out.println(bitCask.get("1"));
        bitCask.merge("testFolder");
        bitCask.put("{ \"station_id\": 2,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 3,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 5,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Mxdium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        System.out.println(bitCask.get("3"));
        System.out.println(bitCask.get("7"));
        File file = new File("testFolder/hint_index");
        System.out.println(file.length());
       bitCask.merge("testFolder");
        bitCask.put("{ \"station_id\": 5,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Mxdium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        bitCask.put("{ \"station_id\": 5,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Mxdium\",\n" +
                "\"status_timestamp\": 1683454444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 0,\n" +
                "\"wind_speed\": 58\n" +
                "}\n" +
                "}");
        System.out.println("after second merge");
        System.out.println(bitCask.get("7"));
        System.out.println(bitCask.get("5"));
        System.out.println(bitCask.get("3"));
        /*BitCaskImpl bitCask  = new BitCaskImpl();
        bitCask.open("testFolder");
        bitCask.rehash();
        System.out.println(bitCask.get("7"));*/




   }
}