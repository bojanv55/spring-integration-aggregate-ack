package me.vukas;

public interface Aggregatable {
    String correlate(String in);
    void aggregate(StringContainer out, String in);
}
