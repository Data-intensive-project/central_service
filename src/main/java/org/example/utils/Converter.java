package org.example.utils;

import org.example.bitcask.MessageSaved;
import org.example.model.Message;

import java.io.*;

public class Converter {
    public  byte[] toByteArray(MessageSaved messageSaved){
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null ;
        try {
            oos = new ObjectOutputStream(bos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            oos.writeObject(messageSaved);
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
