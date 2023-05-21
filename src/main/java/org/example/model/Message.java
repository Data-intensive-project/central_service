package org.example.model;

import java.io.*;

public class Message implements Serializable {
    long station_id;
    long s_no;
    String battery_status;
    long status_timestamp;
    Weather weather;

    public void setBattery_status(String battery_status) {
        this.battery_status = battery_status;
    }

    public void setS_no(long s_no) {
        this.s_no = s_no;
    }


    public void setStation_id(long station_id) {
        this.station_id = station_id;
    }

    public void setStatus_timestamp(long status_timestamp) {
        this.status_timestamp = status_timestamp;
    }

    public long getS_no() {
        return s_no;
    }

    public long getStation_id() {
        return station_id;
    }

    public String getBattery_status() {
        return battery_status;
    }

    public long getStatus_timestamp() {
        return status_timestamp;
    }

    public void setWeather(Weather weather) {
        this.weather = weather;
    }

    public Weather getWeather() {
        return weather;
    }

    @Override
    public String toString() {
        return "{ \"station_id\": " + station_id + ",\n\"s_no\": " + s_no + ",\n" + "\"battery_status\": " + "\"" + battery_status + "\"" + ",\n\"status_timestamp\": " + status_timestamp + ",\n\"weather\": " + weather.toString() + "\n}";
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
    public static Message createFromByteArray(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream ois=null;
        try  {
            ois = new ObjectInputStream(bis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return (Message) ois.readObject();
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Message message = new Message();
        message.setS_no(10);
        byte []arr = message.toByteArray();
        Message test = createFromByteArray(arr);
        System.out.println(test.s_no);
    }
}
