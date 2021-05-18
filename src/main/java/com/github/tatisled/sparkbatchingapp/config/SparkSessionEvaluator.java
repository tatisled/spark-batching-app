package com.github.tatisled.sparkbatchingapp.config;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import static com.github.tatisled.sparkbatchingapp.config.SparkConfig.HADOOP_HOST;

/**
 * Spark Session evaluator
 *
 * @author Tatiana_Slednikova
 * @version 1.0
 * @since 1.0
 */
public class SparkSessionEvaluator {

    public static SparkSession getSparkSession() {
        //spark-submit --packages org.apache.spark:spark-avro_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1
        // --class com.github.tatisled.sparkbatchingapp.SparkApp
        // --master yarn
        // --deploy-mode cluster /home/user_hdfs/spark-batching-app-1.0.jar
        SparkConf sparkConf = new SparkConf().setAppName("spark-app")
//                .setMaster("local[*]")
                .set("spark.hadoop.fs.defaultFS", HADOOP_HOST);

        SparkContext sc = new SparkContext(sparkConf);
        return SparkSession.builder().sparkContext(sc).getOrCreate();
    }

}
