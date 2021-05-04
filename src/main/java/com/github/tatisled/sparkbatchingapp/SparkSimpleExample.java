package com.github.tatisled.sparkbatchingapp;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSimpleExample {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("spark-app").setMaster("local[*]")
                .set("spark.hadoop.fs.defaultFS", "hdfs://hadoop:9000");

        SparkContext sc = new SparkContext(sparkConf);
        SparkSession spark = SparkSession.builder().sparkContext(sc).getOrCreate();

        Dataset<Row> rowDataset = spark.read().csv("hdfs:///dataset/hotels/part-00000-7b2b2c30-eb5e-4ab6-af89-28fae7bdb9e4-c000.csv");
        rowDataset.show();
    }
}
