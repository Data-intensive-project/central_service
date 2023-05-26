package org.example.bitcask;

public class CustomizedThread extends Thread{
    BitCaskImpl bitCask;
    public CustomizedThread(BitCaskImpl bitCask){
        this.bitCask = bitCask;
    }
    public void run(){
        bitCask.merge();
    }
}
