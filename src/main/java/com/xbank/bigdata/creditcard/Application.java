package com.xbank.bigdata.creditcard;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Application {
    public static void main(String[] args) {
        SparkSession sparkSession = getSparkSession();

        Dataset<Row> rowDataset = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("credit_data.csv");

        Dataset<Row> dataset = rowDataset.withColumn("current_ts", from_unixtime(rowDataset.col("current_ts").divide(1000)));
        Dataset<Row> processDF = dataset.groupBy("user_id", "process_type").count();
        Dataset<Row> customers = processDF.groupBy("user_id").pivot("process_type").sum("count").na().fill(0);

        //select customer using bank card but not using QR code
        Dataset<Row> result_Qrcode_notUsing = customers.filter(customers.col("1").$greater(2).and(customers.col("2").equalTo(0)));

        //select customer using credit card but not using virtual credit card
        Dataset<Row> result_notUsingVirtualCard = customers.filter(customers.col("3").$greater(2).and(customers.col("4").equalTo(0))).sort(desc("3"));

        // select process types according to 4 hour time period
        Dataset<Row> resultProcess_timePeriod = dataset.groupBy(window(dataset.col("current_ts"), "4 hour"), dataset.col("process_type")).count()
                .groupBy("window").pivot("process_type").sum("count").na().fill(0)
                .sort(asc("window"));

        //Write to mongo DF1
        MongoSpark.write(resultProcess_timePeriod).mode("append")
                .option("collection", "resultProcess_timePeriod").save();

        //Write to mongo DF2
        MongoSpark.write(result_notUsingVirtualCard).mode("append")
                .option("collection", "result_notUsingVirtualCard").save();

        //Write to mongo DF3
        MongoSpark.write(result_Qrcode_notUsing).mode("append")
                .option("collection", "result_Qrcode_notUsing").save();

    }

    private static SparkSession getSparkSession() {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession
                .builder().master("local")
                .config("spark.mongodb.output.uri", "mongodb://52.246.250.73/credit_card")
                .appName("Credit Card Tracing")
                .getOrCreate();
        return sparkSession;
    }
}
