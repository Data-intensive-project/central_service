package org.example.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.Message;
import org.example.model.Weather;

import java.nio.charset.StandardCharsets;

public class JsonParser {
    public static Message convertStringToMessage(String messageJson){
        ObjectMapper mapper = new ObjectMapper();
        Message message = null;
        try {
             message = mapper.readValue(messageJson, Message.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return message;
    }
    public static void main(String[] args) throws JsonProcessingException {
       /* String test ="{ \"station_id\": 1,\n" +
                "\"s_no\": 1,\n" +
                "\"battery_status\": \"Medium\",\n" +
                "\"status_timestamp\": 1683744444341,\n" +
                "\"weather\": { \"humidity\": 40,\n" +
                "\"temperature\": 121,\n" +
                "\"wind_speed\": 58\n" +
                "}\n " +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        Message message = mapper.readValue(test, Message.class);
        System.out.println(message.toString());*/
        String ahmed = "ahmed";
        byte[] ar = ahmed.getBytes();
        String s = new String(ar, StandardCharsets.UTF_8);
        System.out.println(s);


    }
}
