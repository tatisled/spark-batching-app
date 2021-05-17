package com.github.tatisled.sparkbatchingapp.config;

/**
 * Spark Configuration class
 *
 * @author Tatiana_Slednikova
 * @version 1.0
 * @since 1.0
 */
public class SparkConfig {

    public static final String KAFKA_HOST = "host.docker.internal:9094";
    public static final String EXPEDIA_PATH = "hdfs:///dataset/expedia/*.avro";
    public static final String HADOOP_HOST = "hdfs://hadoop:9000";
}
