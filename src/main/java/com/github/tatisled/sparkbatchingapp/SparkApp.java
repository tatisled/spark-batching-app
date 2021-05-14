package com.github.tatisled.sparkbatchingapp;

import com.github.tatisled.sparkbatchingapp.config.SparkSessionEvaluator;
import com.github.tatisled.sparkbatchingapp.utils.FileFormatsEnum;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;

import static com.github.tatisled.sparkbatchingapp.config.SparkConfig.EXPEDIA_PATH;
import static com.github.tatisled.sparkbatchingapp.config.SparkConfig.KAFKA_HOST;
import static org.apache.spark.sql.functions.col;

/**
 * Main spark application for work with data from hdfs and kafka
 *
 * @author Tatiana_Slednikova
 * @version 1.0
 * @since 1.0
 */
public class SparkApp {

    private static final Logger LOG = Logger.getLogger(SparkApp.class);

    /**
     * Get expedia data from hdfs
     *
     * @param session spark session
     * @return dataset of rows
     */
    public Dataset<Row> getExpediaData(SparkSession session) {
        LOG.info("Loading expedia data...");
        return session.read().format(FileFormatsEnum.AVRO.getFormat())
                .load(EXPEDIA_PATH);
    }

    /**
     * Get hotel data from kafka (parsed)
     *
     * @param session spark session
     * @return dataset of rows
     */
    public Dataset<Row> getHotelData(SparkSession session) {
        LOG.info("Loading hotel data...");
        Dataset<Row> pureHotelData = session.read()
                .format(FileFormatsEnum.KAFKA.getFormat())
                .option("kafka.bootstrap.servers", KAFKA_HOST)
                .option("subscribe", "hotel-data-topic")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
                .select(col("value").cast("string"));

        LOG.info("Loading hotel data schema...");
        String hotelDataJsonSchema = pureHotelData.first().getAs("value").toString();
        LOG.info("Loaded hotel data schema successfully. Hotel data schema: {" + hotelDataJsonSchema + "}");

        return pureHotelData
                .select(col("value").cast("string"))
                .withColumn("value", functions.from_json(col("value"), functions.schema_of_json(hotelDataJsonSchema)))
                .select("value.*");
    }

    /**
     * Get invalid expedia data
     *
     * @param expediaWithHotels expedia data
     * @return dataset of rows
     */
    public Dataset<Row> getInvalidExpedia(Dataset<?> expediaWithHotels) {
        LOG.info("Getting invalid expedia data...");

        var expediaTemp = expediaWithHotels.as("exp")
                .withColumn("srch_shifted", functions.lag("exp.srch_ci", 1).over(Window.partitionBy("exp.hotel_id").orderBy("exp.srch_ci")))
                .withColumn("diff", functions.datediff(col("exp.srch_ci"), col("srch_shifted")))
                .orderBy("hotel_id", "srch_ci");

        return expediaTemp.filter(col("diff").geq(2))
                .filter(col("diff").lt(30));
    }

    /**
     * Get valid expedia data
     *
     * @param expediaData expedia data
     * @param invalidHotelIds list of invalid hotel ids
     * @return dataset of rows
     */
    public Dataset<Row> getValidExpedia(Dataset<Row> expediaData, Object... invalidHotelIds) {
        LOG.info("Getting valid expedia data...");

        return expediaData.filter(functions.not(col("hotel_id").isin(invalidHotelIds)));
    }

    /**
     * Save data to hdfs
     *
     * @param dataset data to be saved
     * @param partitionBy column for partitioning
     * @param format file format
     * @param path file path
     */
    public void saveToHDFS(Dataset<?> dataset, String partitionBy, String format, String path) {
        LOG.info("Saving data to hdfs, dataset: " + dataset + "; format: " + format + "; path: " + path + "; partitioned by: " + partitionBy + "...");

        dataset.write()
                .partitionBy(partitionBy)
                .format(format)
                .option("header", "true")
                .save(path);
    }

    public void run() {
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
