package com.gbhat;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class DateTest {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
        List<Row> rowList = Arrays.asList(
                RowFactory.create("Gr", "7/5/1987", 1),
                RowFactory.create("Sam", "01/08/2020", 0),
                RowFactory.create("Sub", "14/8/1988", 0));
        StructType schema = new StructType()
                .add("Name1", DataTypes.StringType)
                .add("DOB", DataTypes.StringType)
                .add("Is", DataTypes.IntegerType);
        Dataset<Row> ds1 = session.createDataFrame(rowList, schema);
        ds1.show(false);

        Dataset<Row> ds2 = ds1.withColumn("DOB",
                functions.date_format(functions.to_date(ds1.col("DOB"), "d/M/yyyy"), "dd/MM/yyyy"));
        ds2.show(false);

    }
}
