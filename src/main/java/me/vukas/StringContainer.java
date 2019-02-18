package me.vukas;

public class StringContainer {
    private String value = "";

    public String getValue() {
        return value;
    }

    public void aggregateWith(String value) {
        this.value += "__aggr__" + value;
    }
}
