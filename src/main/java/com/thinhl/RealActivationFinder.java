package com.thinhl;

import com.thinhl.model.PhoneActivationRecord;
import com.thinhl.utils.ActivationFinderUtils;
import com.thinhl.utils.Parser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import static com.thinhl.utils.ActivationFinderUtils.loadCSVFile;
import static com.thinhl.utils.ActivationFinderUtils.writeCSVFile;


public class RealActivationFinder {


    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            throw new Exception("Usage need 2 parameter csvInputFile csvOutputFile");
        }

        String csvInput = args[0];
        String csvOutput = args[1];

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("PhoneActivationFinder");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        StructType inputSchema = new StructType(new StructField[]{
                new StructField("PHONE_NUMBER", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ACTIVATION_DATE", DataTypes.DateType, true, Metadata.empty()),
                new StructField("DEACTIVATION_DATE", DataTypes.DateType, true, Metadata.empty())
        });

        Dataset<Row> df = loadCSVFile(csvInput, sparkSession, inputSchema);


        JavaRDD<PhoneActivationRecord> jRDD = df.javaRDD().map(new Parser());
        JavaRDD<Row> data = jRDD
                .groupBy(PhoneActivationRecord::getPhoneNumber)
                .map((Function<Tuple2<String, Iterable<PhoneActivationRecord>>, Row>) stringIterableTuple2 -> {
                    return RowFactory.create(stringIterableTuple2._1, ActivationFinderUtils.findRealActivationDate(stringIterableTuple2._2.iterator()));
                });


        StructType resultSchema = new StructType(new StructField[]{
                new StructField("PHONE_NUMBER", DataTypes.StringType, true, Metadata.empty()),
                new StructField("REAL_ACTIVATION_DATE", DataTypes.StringType, true, Metadata.empty())
        });

        writeCSVFile(csvOutput, resultSchema, data, sparkSession);
    }
}
