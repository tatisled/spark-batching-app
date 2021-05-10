package com.github.tatisled.sparkbatchingapp;

import com.github.tatisled.sparkbatchingapp.config.SparkSessionEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSimpleExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSessionEvaluator.getSparkSession();

        Dataset<Row> rowDataset = spark.read().csv("hdfs:///dataset/hotels/part-00000-7b2b2c30-eb5e-4ab6-af89-28fae7bdb9e4-c000.csv");
        rowDataset.show();
    }
}
