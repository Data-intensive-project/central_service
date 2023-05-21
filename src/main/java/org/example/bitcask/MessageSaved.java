package org.example.bitcask;

import org.apache.kafka.common.protocol.types.Field;
import org.example.model.Message;

import java.io.*;

public class MessageSaved implements Serializable {
    KeyDirectoryRecord keyDirectoryRecord;
    Message value;
    String key;
    MessageSaved(KeyDirectoryRecord keyDirectoryRecord,Message value,String key){
        this.keyDirectoryRecord=keyDirectoryRecord;
        this.value=value;
        this.key=key;
    }


    public  byte[] toByteArray(){
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null ;
        try {
            oos = new ObjectOutputStream(bos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            oos.writeObject(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return bos.toByteArray();
    }
    public static MessageSaved createFromByteArray(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream ois=null;
        try  {
            ois = new ObjectInputStream(bis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return (MessageSaved) ois.readObject();
    }
}
