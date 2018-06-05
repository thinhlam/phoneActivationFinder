package com.thinhl;

import com.thinhl.model.PhoneActivationRecord;
import com.thinhl.utils.ActivationFinderUtils;
import com.thinhl.utils.Parser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;


public class Main {

    public static void loadCSVFile() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("PhoneActivationFinder");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        StructType inputSchema = new StructType(new StructField[]{
                new StructField("PHONE_NUMBER", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACTIVATION_DATE", DataTypes.DateType, true, Metadata.empty()),
                new StructField("DEACTIVATION_DATE", DataTypes.DateType, true, Metadata.empty())
        });

        Dataset<Row> df = sparkSession.read()
                .format("com.databricks.spark.csv")
                .schema(inputSchema)
                .option("header", "true")
                .load("src/main/resources/input.csv");

        JavaRDD<PhoneActivationRecord> jRDD = df.javaRDD().map(new Parser());

        JavaRDD<Row> map = jRDD
                .groupBy(PhoneActivationRecord::getPhoneNumber)
                .map((Function<Tuple2<String, Iterable<PhoneActivationRecord>>, Row>) stringIterableTuple2 -> {
                    return RowFactory.create(stringIterableTuple2._1, ActivationFinderUtils.findRealActivationDate(stringIterableTuple2._2.iterator()));
                });

        StructType resultSchema = new StructType(new StructField[]{
                new StructField("PHONE_NUMBER", DataTypes.StringType, true, Metadata.empty()),
                new StructField("REAL_ACTIVATION_DATE", DataTypes.StringType, true, Metadata.empty())
        });

        sparkSession
                .createDataFrame(map, resultSchema)
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .csv("src/main/resources/output.csv");
    }

    public static void main(String[] args) {
        loadCSVFile();
    }
}
