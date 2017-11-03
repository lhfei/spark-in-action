package org.apache.spark.examples.sql.hive;

import cn.lhfei.spark.BaseApp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Created by lhfei on 8/29/17.
 */
public class KylinJoinHiveBillonRowsApp extends BaseApp {


    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

       /* Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://master1.cloud.cn:3306")
                .option("dbtable", "defalut.KYLIN_ACCOUNT")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "lhfei")
                .option("password", "Lhfei@123")
                .load();

        jdbcDF.show();*/


        Dataset<Row> sqlDF = spark.sql("select count(*) from kylin_account");

        sqlDF.show();
    }
}
