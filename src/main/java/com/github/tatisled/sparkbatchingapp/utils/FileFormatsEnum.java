package com.github.tatisled.sparkbatchingapp.utils;

public enum FileFormatsEnum {
    KAFKA("kafka"),
    AVRO("avro");

    private final String format;

    FileFormatsEnum(String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }
}
