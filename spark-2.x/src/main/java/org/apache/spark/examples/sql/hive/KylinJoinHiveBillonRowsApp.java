package org.apache.spark.examples.sql.hive;

import cn.lhfei.spark.TopBaseApp;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by lhfei on 8/29/17.
 */
public class KylinJoinHiveBillonRowsApp extends TopBaseApp {

    private static final Logger LOG = LoggerFactory.getLogger(KylinJoinHiveBillonRowsApp.class);

    public static void main(String[] args) {
        String warehouseLocation = new File("hdfs://master1.cloud.cn:9000/user/hive/warehouse").getAbsolutePath();
        warehouseLocation = "hdfs://master1.cloud.cn:9000/user/hive/warehouse";
        LOG.info("=={}", warehouseLocation);
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        /*Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://master1.cloud.cn:3306")
                .option("dbtable", "defalut.KYLIN_ACCOUNT")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "lhfei")
                .option("password", "Lhfei@123")
                .load();

        jdbcDF.show();*/

        SQLContext sqlContext = spark.sqlContext();
        SessionHiveMetaStoreClient sessionHiveMetaStoreClient;

        sqlContext.tables().show();

        spark.catalog().listDatabases();
        sqlContext.tables().show();


        LOG.info(">>>>{}>>>>", spark.sparkContext().getConf().get("spark.sql.warehouse.dir"));
        sqlContext.tables("lhfei").foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                LOG.info("======= {} ======", row.getString(1));
            }
        });
        Dataset<Row> sqlDF = spark.sql("select count(*) from lhfei.bank");

        sqlDF.show();
    }
}
