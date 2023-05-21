package org.example.model;


import java.io.Serializable;

public class Weather implements Serializable {
    int humidity;
    int temperature;
    int wind_speed;

    Weather(int humidity, int temperature, int windSpeed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.wind_speed = windSpeed;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public void setWind_speed(int wind_speed) {
        this.wind_speed = wind_speed;
    }

    public int getHumidity() {
        return humidity;
    }

    public int getTemperature() {
        return temperature;
    }

    public int getWind_speed() {
        return wind_speed;
    }

    public Weather() {

    }

    @Override
    public String toString() {
        return "{ \"humidity\": " + humidity + ",\n" + "\"temperature\": " + temperature + ",\n" + "\"wind_speed\": " + wind_speed + "\n}";
    }
}
