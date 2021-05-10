package com.github.tatisled.sparkbatchingapp;

import com.github.tatisled.sparkbatchingapp.config.SparkSessionEvaluator;
import com.github.tatisled.sparkbatchingapp.utils.FileFormatsEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class SparkApp {

    protected Dataset<Row> getExpediaData(SparkSession session) {
        return session.read().format(FileFormatsEnum.AVRO.getFormat())
                .load("hdfs:///dataset/expedia/*.avro");
    }

    protected Dataset<Row> getHotelData(SparkSession session) {
        Dataset<Row> pureHotelData = session.read()
                .format(FileFormatsEnum.KAFKA.getFormat())
                .option("kafka.bootstrap.servers", "host.docker.internal:9094")
                .option("subscribe", "hotel-data-topic")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
                .select(col("value").cast("string"));

        String hotelDataJsonSchema = pureHotelData.first().getAs("value").toString();

        return pureHotelData
                .select(col("value").cast("string"))
                .withColumn("value", functions.from_json(col("value"), functions.schema_of_json(hotelDataJsonSchema)))
                .select("value.*");
    }

    protected Dataset<Row> getInvalidExpedia(Dataset<?> expediaWithHotels) {
        var expediaTemp = expediaWithHotels.as("exp")
                .withColumn("srch_shifted", functions.lag("exp.srch_ci", 1).over(Window.orderBy("exp.hotel_id", "exp.srch_ci")))
                .withColumn("diff", functions.datediff(col("exp.srch_ci"), col("srch_shifted")))
                .orderBy("hotel_id", "srch_ci");

        return expediaTemp.filter(col("diff").geq(2))
                .filter(col("diff").lt(30));
    }

    protected Dataset<Row> getValidExpedia(Dataset<Row> expediaData, Object... invalidHotelIds) {
        return expediaData.filter(functions.not(col("hotel_id").isin(invalidHotelIds)));
    }

    protected void saveToHDFS(Dataset<?> dataset, String partitionBy, String format, String path) {
        dataset.write()
                .partitionBy(partitionBy)
                .format(format)
                .option("header", "true")
                .save(path);
    }

    protected void run() {
        SparkSession spark = SparkSessionEvaluator.getSparkSession();

        Dataset<Row> expediaData = getExpediaData(spark);
        Dataset<Row> hotelData = getHotelData(spark);

        var expediaWithHotels = expediaData.as("exp")
                .join(hotelData.as("htl"), col("exp.hotel_id").equalTo(col("htl.Id")));

        expediaWithHotels.cache();

        var expediaInvalid = getInvalidExpedia(expediaWithHotels);

        var invalidHotelIds = expediaInvalid.select("hotel_id").collectAsList().stream().map(row -> row.getAs("hotel_id").toString()).toArray();

        var expediaOkWithHotels = getValidExpedia(expediaWithHotels, invalidHotelIds);
        var expediaOK = getValidExpedia(expediaData, invalidHotelIds)
                .withColumn("year", functions.year(col("srch_ci")));

        //For screenshot
        expediaInvalid.show();
        //For screenshot
        expediaOkWithHotels.select("Country")
                .groupBy("Country").count()
                .show();
        //For screenshot
        expediaOkWithHotels.select("City")
                .groupBy("City").count()
                .show();

        saveToHDFS(expediaOK, "year", FileFormatsEnum.AVRO.getFormat(), "hdfs:///result/");
    }

    public static void main(String[] args) {
        new SparkApp().run();
    }
}
