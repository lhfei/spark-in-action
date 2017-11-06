package org.apache.spark.examples.sql.hive;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;

/**
 * Created by lhfei on 11/5/17.
 */
public class SparkHiveTablesApp {

    private static final Logger LOG = LoggerFactory.getLogger(SparkHiveTablesApp.class);

    public static void main(String[] args) {
        String sparkWaarehouse = "hdfs://master1.cloud.cn:9000/user/hive/warehouse";

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Hive Inner Table Example")
                .config("spark.sql.warehouse.dir", sparkWaarehouse)
                .enableHiveSupport()
                .getOrCreate();


        SQLContext sqlContext = spark.sqlContext();

        sqlContext.sql("select EDUCATION, avg(balance) as BALANCE, " +
                "count(EDUCATION) as TOTAL from lhfei.bank " +
                "group by EDUCATION order by balance desc")
                .show();


        Dataset<Row> result = sqlContext.sql("select * from lhfei.bank");
        result = result.filter(col("balance").gt(1700));

        LOG.info("Filter Result: {}", result.count());

        result.show();


    }
}
