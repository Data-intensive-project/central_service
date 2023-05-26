package org.example.bitcask;

import org.example.model.Message;

public interface BitCask {

    public void open(String DirectoryName);
    public Message get(String key);
    public void put (String value);
    public void merge();

}
