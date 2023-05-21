package org.example.bitcask;

import org.example.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class HintRecord implements Serializable {
    String key;
    KeyDirectoryRecord keyDirectoryRecord;

    HintRecord(String key, KeyDirectoryRecord keyDirectoryRecord) {
        this.key = key;
        this.keyDirectoryRecord = keyDirectoryRecord;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
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
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return bos.toByteArray();
    }
    public static HintRecord createFromByteArray(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream ois=null;
        try  {
            ois = new ObjectInputStream(bis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return (HintRecord) ois.readObject();
    }
    public static void main(String [] args) throws IOException, ClassNotFoundException {
       /* File file = new File("hint");
        file.createNewFile();


        HintRecord  hintRecord2 = new HintRecord();
        hintRecord2.key="test";
        HintRecord hintRecord3 = new HintRecord();
        hintRecord3.key="fadsf";
        FileOutputStream output = new FileOutputStream("h", true);
        try {
            output.write(hintRecord2.toByteArray());
        } finally {
            output.close();
        }       // Files.write(Paths.get(file.getPath()), hintRecord3.toByteArray(), StandardOpenOption.APPEND);
        HintRecord re =HintRecord.createFromByteArray(hintRecord2.toByteArray());
        System.out.println(re.key);
        FileInputStream fi = new FileInputStream(new File("tes"));
        ObjectInputStream oi = new ObjectInputStream(fi);

        HintRecord ahmed= (HintRecord)oi.readObject();
        System.out.println(ahmed);
      //  HintRecord hintRecord1 = (HintRecord)oi.readObject();
       // System.out.println(hintRecord1.key);

        file.delete();
*/

    }
}
