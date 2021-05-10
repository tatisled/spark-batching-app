package com.github.tatisled.sparkbatchingapp.config;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkSessionEvaluator {

    public static SparkSession getSparkSession() {
        SparkConf sparkConf = new SparkConf().setAppName("spark-app").setMaster("local[*]")
                .set("spark.hadoop.fs.defaultFS", "hdfs://hadoop:9000");

        SparkContext sc = new SparkContext(sparkConf);
        return SparkSession.builder().sparkContext(sc).getOrCreate();
    }

}
