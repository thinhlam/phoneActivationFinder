package com.thinhl.utils;

import com.thinhl.model.PhoneActivationRecord;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class ActivationFinderUtils {

    public static String DATE_PATTERN = "yyyy-MM-dd";

    public static String findRealActivationDate(Iterator<PhoneActivationRecord> records) {
        if (records == null || !records.hasNext())
            return "";

        List<PhoneActivationRecord> recordLst = IteratorUtils.toList(records);
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_PATTERN);


        if (recordLst.size() == 1) {
            return formatter.format(recordLst.get(0).getActivationDate());
        }

        recordLst.sort(Comparator.comparing(PhoneActivationRecord::getActivationDate));

        Date realActivationDate = recordLst.get(0) != null ? recordLst.get(0).getActivationDate() : null;
        for (int i = 0; i < recordLst.size(); i++) {
            if (i + 1 < recordLst.size() && recordLst.get(i).getDeActivationDate() != null) {
                if (!(recordLst.get(i).getDeActivationDate().getTime() == recordLst.get(i + 1).getActivationDate().getTime())) {
                    realActivationDate = recordLst.get(i + 1).getActivationDate();
                }
            }
        }
        return formatter.format(realActivationDate);

    }

    public static Dataset<Row> loadCSVFile(String input, SparkSession sparkSession, StructType inputSchema) {
        return sparkSession.read()
                .format("com.databricks.spark.csv")
                .schema(inputSchema)
                .option("header", "true")
                .load(input);
    }

    public static void writeCSVFile(String output, StructType resultSchema, JavaRDD data, SparkSession sparkSession) {
        sparkSession
                .createDataFrame(data, resultSchema)
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(output);
    }

}
