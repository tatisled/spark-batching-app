import com.github.tatisled.sparkbatchingapp.SparkApp;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class SparkAppTest {

    private static SparkSession spark;

    private StructType getExpediaStructType() {
        return new StructType()
                .add("id", DataTypes.LongType)
//        .add("date_time", DataTypes.StringType)
//        .add("site_name", DataTypes.IntegerType)
//        .add("posa_continent", DataTypes.IntegerType)
//        .add("user_location_country", DataTypes.IntegerType)
//        .add("user_location_region", DataTypes.IntegerType)
//        .add("user_location_city", DataTypes.IntegerType)
//        .add("orig_destination_distance", DataTypes.DoubleType)
//        .add("user_id", DataTypes.IntegerType)
//        .add("is_mobile", DataTypes.IntegerType)
//        .add("is_package", DataTypes.IntegerType)
//        .add("channel", DataTypes.IntegerType)
                .add("srch_ci", DataTypes.StringType)
                .add("srch_co", DataTypes.StringType)
//        .add("srch_adults_cnt", DataTypes.IntegerType)
//        .add("srch_children_cnt", DataTypes.IntegerType)
//        .add("srch_rm_cnt", DataTypes.IntegerType)
//        .add("srch_destination_id", DataTypes.IntegerType)
//        .add("srch_destination_type_id", DataTypes.IntegerType)
                .add("hotel_id", DataTypes.LongType);
    }

    private List<Row> fillExpediaWithTestData() {
        List<Row> expediaRows = new ArrayList<>();
        StructType structType = getExpediaStructType();

        expediaRows.add(new GenericRowWithSchema(new Object[]{1L, "2001-03-02", "2001-03-20", 1000L}, structType));
        expediaRows.add(new GenericRowWithSchema(new Object[]{2L, "2001-03-22", "2001-03-23", 1001L}, structType));
        expediaRows.add(new GenericRowWithSchema(new Object[]{3L, "2001-04-10", "2001-04-20", 1001L}, structType));
        expediaRows.add(new GenericRowWithSchema(new Object[]{4L, "2001-05-05", "2001-05-23", 1002L}, structType));
        expediaRows.add(new GenericRowWithSchema(new Object[]{5L, "2001-05-25", "2001-05-26", 1002L}, structType));
        expediaRows.add(new GenericRowWithSchema(new Object[]{6L, "2001-06-05", "2001-06-26", 1003L}, structType));

        return expediaRows;
    }

    private void checkExpediaData(Dataset<Row> expectedResult, Dataset<Row> actualResult) {
        expectedResult.show();
        actualResult.show();

        Assertions.assertArrayEquals(expectedResult.select("id").collectAsList().stream().map(row -> row.getAs("id").toString()).toArray()
                , actualResult.select("id").collectAsList().stream().map(row -> row.getAs("id").toString()).toArray());
        Assertions.assertArrayEquals(expectedResult.select("srch_ci").collectAsList().stream().map(row -> row.getAs("srch_ci").toString()).toArray()
                , actualResult.select("srch_ci").collectAsList().stream().map(row -> row.getAs("srch_ci").toString()).toArray());
        Assertions.assertArrayEquals(expectedResult.select("srch_co").collectAsList().stream().map(row -> row.getAs("srch_co").toString()).toArray()
                , actualResult.select("srch_co").collectAsList().stream().map(row -> row.getAs("srch_co").toString()).toArray());
        Assertions.assertArrayEquals(expectedResult.select("hotel_id").collectAsList().stream().map(row -> row.getAs("hotel_id").toString()).toArray()
                , actualResult.select("hotel_id").collectAsList().stream().map(row -> row.getAs("hotel_id").toString()).toArray());
    }

    @BeforeAll
    public static void setUp() {
        SparkConf sparkConf = new SparkConf().setAppName("spark-app").setMaster("local[*]");

        SparkContext sc = new SparkContext(sparkConf);
        spark = SparkSession.builder().sparkContext(sc).getOrCreate();
    }

    @Test
    public void testInvalidExpedia() {
        StructType structType = getExpediaStructType();

        List<Row> expediaRows = fillExpediaWithTestData();

        List<Row> expectedRows = new ArrayList<>();
        expectedRows.add(new GenericRowWithSchema(new Object[]{3L, "2001-04-10", "2001-04-20", 1001L}, structType));
        expectedRows.add(new GenericRowWithSchema(new Object[]{5L, "2001-05-25", "2001-05-26", 1002L}, structType));

        Dataset<Row> expediaWithInvalid = spark.createDataFrame(expediaRows, structType);

        Dataset<Row> expectedResult = spark.createDataFrame(expectedRows, structType);
        Dataset<Row> actualResult = new SparkApp().getInvalidExpedia(expediaWithInvalid);

        checkExpediaData(expectedResult, actualResult);
    }

    @Test
    public void testEmptyInvalidExpedia() {
        StructType structType = getExpediaStructType();

        List<Row> expediaRows = new ArrayList<>();
        List<Row> expectedRows = new ArrayList<>();

        Dataset<Row> expediaWithInvalid = spark.createDataFrame(expediaRows, structType);

        Dataset<Row> expectedResult = spark.createDataFrame(expectedRows, structType);
        Dataset<Row> actualResult = new SparkApp().getInvalidExpedia(expediaWithInvalid);

        checkExpediaData(expectedResult, actualResult);
    }

    @Test
    public void testOneInvalidExpedia() {
        StructType structType = getExpediaStructType();

        List<Row> expediaRows = new ArrayList<>();
        expediaRows.add(new GenericRowWithSchema(new Object[]{1L, "2001-03-02", "2001-03-20", 1000L}, structType));

        List<Row> expectedRows = new ArrayList<>();

        Dataset<Row> expediaWithInvalid = spark.createDataFrame(expediaRows, structType);

        Dataset<Row> expectedResult = spark.createDataFrame(expectedRows, structType);
        Dataset<Row> actualResult = new SparkApp().getInvalidExpedia(expediaWithInvalid);

        checkExpediaData(expectedResult, actualResult);
    }

    @Test
    public void testNotFoundInvalidExpedia() {
        StructType structType = getExpediaStructType();

        List<Row> expediaRows = new ArrayList<>();

        expediaRows.add(new GenericRowWithSchema(new Object[]{1L, "2001-03-02", "2001-03-20", 1000L}, structType));
        expediaRows.add(new GenericRowWithSchema(new Object[]{6L, "2001-06-05", "2001-06-26", 1003L}, structType));

        List<Row> expectedRows = new ArrayList<>();

        Dataset<Row> expediaWithInvalid = spark.createDataFrame(expediaRows, structType);

        Dataset<Row> expectedResult = spark.createDataFrame(expectedRows, structType);
        Dataset<Row> actualResult = new SparkApp().getInvalidExpedia(expediaWithInvalid);

        checkExpediaData(expectedResult, actualResult);
    }

    @Test
    public void testValidExpedia() {
        StructType structType = getExpediaStructType();

        List<Row> expediaRows = fillExpediaWithTestData();

        String[] invalidHotelIds = new String[]{"1001", "1002"};

        List<Row> expectedRows = new ArrayList<>();
        expectedRows.add(new GenericRowWithSchema(new Object[]{1L, "2001-03-02", "2001-03-20", 1000L}, structType));
        expectedRows.add(new GenericRowWithSchema(new Object[]{6L, "2001-06-05", "2001-06-26", 1003L}, structType));

        Dataset<Row> expediaWithInvalid = spark.createDataFrame(expediaRows, structType);

        Dataset<Row> expectedResult = spark.createDataFrame(expectedRows, structType);
        Dataset<Row> actualResult = new SparkApp().getValidExpedia(expediaWithInvalid, invalidHotelIds);

        checkExpediaData(expectedResult, actualResult);
    }

    @Test
    public void testEmptyHotelsValidExpedia() {
        StructType structType = getExpediaStructType();

        List<Row> expediaRows = fillExpediaWithTestData();

        String[] invalidHotelIds = new String[]{};

        List<Row> expectedRows = fillExpediaWithTestData();

        Dataset<Row> expediaWithInvalid = spark.createDataFrame(expediaRows, structType);

        Dataset<Row> expectedResult = spark.createDataFrame(expectedRows, structType);
        Dataset<Row> actualResult = new SparkApp().getValidExpedia(expediaWithInvalid, invalidHotelIds);

        checkExpediaData(expectedResult, actualResult);
    }

    @Test
    public void testEmptyExpediaValidExpedia() {
        StructType structType = getExpediaStructType();

        List<Row> expediaRows = new ArrayList<>();

        String[] invalidHotelIds = new String[]{"1000", "1003"};

        List<Row> expectedRows = new ArrayList<>();

        Dataset<Row> expediaWithInvalid = spark.createDataFrame(expediaRows, structType);

        Dataset<Row> expectedResult = spark.createDataFrame(expectedRows, structType);
        Dataset<Row> actualResult = new SparkApp().getValidExpedia(expediaWithInvalid, invalidHotelIds);

        checkExpediaData(expectedResult, actualResult);
    }

    @Test
    public void testOneValidExpedia() {
        StructType structType = getExpediaStructType();

        List<Row> expediaRows = fillExpediaWithTestData();

        String[] invalidHotelIds = new String[]{"1000", "1002", "1003"};

        List<Row> expectedRows = new ArrayList<>();
        expectedRows.add(new GenericRowWithSchema(new Object[]{2L, "2001-03-22", "2001-03-23", 1001L}, structType));
        expectedRows.add(new GenericRowWithSchema(new Object[]{3L, "2001-04-10", "2001-04-20", 1001L}, structType));

        Dataset<Row> expediaWithInvalid = spark.createDataFrame(expediaRows, structType);

        Dataset<Row> expectedResult = spark.createDataFrame(expectedRows, structType);
        Dataset<Row> actualResult = new SparkApp().getValidExpedia(expediaWithInvalid, invalidHotelIds);

        checkExpediaData(expectedResult, actualResult);
    }

    @AfterAll
    public static void tearDown(){
        spark.stop();
    }

}
