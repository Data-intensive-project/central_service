package org.example.bitcask;

import java.io.*;

public class KeyDirectoryRecord implements Serializable{
    long fileId;
    int valueSize;
    long valuePos;
    long timeStamp;
    KeyDirectoryRecord( long fileId , int valueSize, long valuePos,long timeStamp){
        this.valueSize=valueSize;
        this.fileId=fileId;
        this.valuePos=valuePos;
        this.timeStamp=timeStamp;

    }

    public KeyDirectoryRecord() {

    }
    public static KeyDirectoryRecord createFromByteArray(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream ois=null;
        try  {
            ois = new ObjectInputStream(bis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return (KeyDirectoryRecord) ois.readObject();
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

}
