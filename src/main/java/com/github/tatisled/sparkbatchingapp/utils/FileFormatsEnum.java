package com.github.tatisled.sparkbatchingapp.utils;

/**
 * File formats
 *
 * @author Tatiana_Slednikova
 * @version 1.0
 * @since 1.0
 */
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
