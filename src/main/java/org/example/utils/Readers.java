package org.example.utils;

import org.example.bitcask.HintRecord;
import org.example.bitcask.KeyDirectoryRecord;
import org.example.bitcask.MessageSaved;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Readers {
    public static MessageSaved ReadMessageSavedObject(String filename) throws IOException, ClassNotFoundException {
        FileInputStream fi = null;
        ObjectInputStream oi=null;
        try {
            fi = new FileInputStream(new File(filename));
            oi = new ObjectInputStream(fi);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return  (MessageSaved) oi.readObject();
    }
    public static ArrayList<MessageSaved> ReadAllMessageSavedObjects(String filePath) throws IOException {
        byte[] size = new byte[4];
        ArrayList<MessageSaved> hintRecords = new ArrayList<>();
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(filePath
            );
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        int rc =-1;
        try {
            rc = fileInputStream.read(size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        while (rc!=-1){
            int valSize = convertByteArrayToInt(size);
            byte[] data = new byte[valSize];
            try {
                rc= fileInputStream.read(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            MessageSaved hint = null;
            try {
                hint = MessageSaved.createFromByteArray(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            hintRecords.add(hint);
            rc = fileInputStream.read(size);
        }
        return hintRecords;
    }

    public static ArrayList<HintRecord> ReadAllHintRecord(String filePath) throws IOException, ClassNotFoundException {
        byte[] size = new byte[4];
        ArrayList<HintRecord> hintRecords = new ArrayList<>();
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(filePath
            );
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        int rc =-1;
        try {
             rc = fileInputStream.read(size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        while (rc!=-1){
            int valSize = convertByteArrayToInt(size);
            byte[] data = new byte[valSize];
            try {
                rc= fileInputStream.read(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            HintRecord hint = HintRecord.createFromByteArray(data);
            hintRecords.add(hint);
            rc = fileInputStream.read(size);
        }
        return hintRecords;
    }
    private static byte[] intToByteArray ( final int i ) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(i);
        dos.flush();
        return bos.toByteArray();
    }
    private static int convertByteArrayToInt(byte[] intBytes){
        ByteBuffer byteBuffer = ByteBuffer.wrap(intBytes);
        return byteBuffer.getInt();
    }
}
