package cn.lhfei.spark.df;

import cn.lhfei.spark.TopBaseApp;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by lhfei on 8/29/17.
 */
public class KylinJoinBillionRowsApp extends TopBaseApp {
    private static final Logger LOG = LoggerFactory.getLogger(KylinJoinBillionRowsApp.class);

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Spark Dataframes, Dataset and SQL.")
                .getOrCreate();

        Dataset<Row> kylinAccountDF = spark.read().csv(HDFS.DEFAULT_FS + "/user/hive/warehouse/kylin_account/DEFAULT.KYLIN_ACCOUNT.csv");
        Dataset<Row> kylinCalDF = spark.read().csv(HDFS.DEFAULT_FS + "/user/hive/warehouse/kylin_cal_dt/DEFAULT.KYLIN_CAL_DT.csv");
        Dataset<Row> kylinGroupsDF = spark.read().csv(HDFS.DEFAULT_FS + "/user/hive/warehouse/kylin_category_groupings/DEFAULT.KYLIN_CATEGORY_GROUPINGS.csv");
        Dataset<Row> kylinCountryDF = spark.read().csv(HDFS.DEFAULT_FS + "/user/hive/warehouse/kylin_country/DEFAULT.KYLIN_COUNTRY.csv");
        Dataset<Row> kylinSalesDF = spark.read().csv(HDFS.DEFAULT_FS + "/user/hive/warehouse/kylin_sales/DEFAULT.KYLIN_SALES.csv");

        kylinAccountDF.show();
        kylinCalDF.show();
        kylinGroupsDF.show();
        kylinCountryDF.show();
        kylinSalesDF.show();

        String[] accountColNames = new String[]{"ACCOUNT_ID", "ACCOUNT_BUYER_LEVEL", "ACCOUNT_SELLER_LEVEL", "ACCOUNT_COUNTRY", "EMPTY_CL"};
        String[] calColNames = new String[]{"CAL_DT", "YEAR_BEG_DT", "MONTH_BEG_DT", "WEEK_BEG_DT", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "_c13", "_c14", "_c15", "_c16", "_c17", "_c18", "_c19", "_c20", "_c21", "_c22", "_c23", "_c24", "_c25", "_c26", "_c27", "_c28", "_c29", "_c30", "_c31", "_c32", "_c33", "_c34", "_c35", "_c36", "_c37", "_c38", "_c39", "_c40", "_c41", "_c42", "_c43", "_c44", "_c45", "_c46", "_c47", "_c48", "_c49", "_c50", "_c51", "_c52", "_c53", "_c54", "_c55", "_c56", "_c57", "_c58", "_c59", "_c60", "_c61", "_c62", "_c63", "_c64", "_c65", "_c66", "_c67", "_c68", "_c69", "_c70", "_c71", "_c72", "_c73", "_c74", "_c75", "_c76", "_c77", "_c78", "_c79", "_c80", "_c81", "_c82", "_c83", "_c84", "_c85", "_c86", "_c87", "_c88", "_c89", "_c90", "_c91", "_c92", "_c93", "_c94", "_c95", "_c96", "_c97", "_c98", "_c99" };
        String[] groupsColNames = new String[]{"LEAF_CATEG_ID", "SITE_ID", "USER_DEFINED_FIELD1", "USER_DEFINED_FIELD3", "META_CATEG_NAME", "CATEG_LVL2_NAME", "CATEG_LVL3_NAME", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "_c13", "_c14", "_c15", "_c16", "_c17", "_c18", "_c19", "_c20", "_c21", "_c22", "_c23", "_c24", "_c25", "_c26", "_c27", "_c28", "_c29", "_c30", "_c31", "_c32", "_c33", "_c34", "_c35"};
        String[] countryColNames = new String[]{"COUNTRY", "NAME", "_c2", "_c3"};
        String[] salesColNames = new String[]{"TRANS_ID", "PART_DT",
                "LSTG_FORMAT_NAME", "LEAF_CATEG_ID", "LSTG_SITE_ID",
                "PRICE", "SELLER_ID", "BUYER_ID", "OPS_USER_ID", "OPS_REGION", "_c10", "_c11"};

        kylinAccountDF.toDF(accountColNames);
        kylinCalDF.toDF(calColNames);
        kylinGroupsDF.toDF(groupsColNames);
        kylinCountryDF.toDF(countryColNames);
        kylinSalesDF.toDF(salesColNames);


        kylinSalesDF.show();

        // Register the Dataframe as a SQL temporary view.
        kylinSalesDF.createOrReplaceTempView("kylin_sales");
        kylinCalDF.createOrReplaceTempView("kylin_cal_dt");
        kylinGroupsDF.createOrReplaceTempView("kylin_category_groupings");
        kylinAccountDF.createOrReplaceTempView("kylin_account");
        kylinCountryDF.createOrReplaceTempView("kylin_country");

        String sql = "SELECT																																"
                +"KYLIN_SALES.TRANS_ID as KYLIN_SALES_TRANS_ID                                                                                          "
                +",KYLIN_SALES.PART_DT as KYLIN_SALES_PART_DT                                                                                           "
                +",KYLIN_SALES.LEAF_CATEG_ID as KYLIN_SALES_LEAF_CATEG_ID                                                                               "
                +",KYLIN_SALES.LSTG_SITE_ID as KYLIN_SALES_LSTG_SITE_ID                                                                                 "
                +",KYLIN_CATEGORY_GROUPINGS.META_CATEG_NAME as KYLIN_CATEGORY_GROUPINGS_META_CATEG_NAME                                                 "
                +",KYLIN_CATEGORY_GROUPINGS.CATEG_LVL2_NAME as KYLIN_CATEGORY_GROUPINGS_CATEG_LVL2_NAME                                                 "
                +",KYLIN_CATEGORY_GROUPINGS.CATEG_LVL3_NAME as KYLIN_CATEGORY_GROUPINGS_CATEG_LVL3_NAME                                                 "
                +",KYLIN_SALES.LSTG_FORMAT_NAME as KYLIN_SALES_LSTG_FORMAT_NAME                                                                         "
                +",KYLIN_SALES.SELLER_ID as KYLIN_SALES_SELLER_ID                                                                                       "
                +",KYLIN_SALES.BUYER_ID as KYLIN_SALES_BUYER_ID                                                                                         "
                +",BUYER_ACCOUNT.ACCOUNT_BUYER_LEVEL as BUYER_ACCOUNT_ACCOUNT_BUYER_LEVEL                                                               "
                +",SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL as SELLER_ACCOUNT_ACCOUNT_SELLER_LEVEL                                                           "
                +",BUYER_ACCOUNT.ACCOUNT_COUNTRY as BUYER_ACCOUNT_ACCOUNT_COUNTRY                                                                       "
                +",SELLER_ACCOUNT.ACCOUNT_COUNTRY as SELLER_ACCOUNT_ACCOUNT_COUNTRY                                                                     "
                +",BUYER_COUNTRY.NAME as BUYER_COUNTRY_NAME                                                                                             "
                +",SELLER_COUNTRY.NAME as SELLER_COUNTRY_NAME                                                                                           "
                +",KYLIN_SALES.OPS_USER_ID as KYLIN_SALES_OPS_USER_ID                                                                                   "
                +",KYLIN_SALES.OPS_REGION as KYLIN_SALES_OPS_REGION                                                                                     "
                +",KYLIN_SALES.PRICE as KYLIN_SALES_PRICE                                                                                               "
                +"FROM KYLIN_SALES as KYLIN_SALES                                                                                               		"
                +"INNER JOIN KYLIN_CAL_DT as KYLIN_CAL_DT                                                                                       		"
                +"ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.CAL_DT                                                                                          "
                +"INNER JOIN KYLIN_CATEGORY_GROUPINGS as KYLIN_CATEGORY_GROUPINGS                                                               		"
                +"ON KYLIN_SALES.LEAF_CATEG_ID = KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND KYLIN_SALES.LSTG_SITE_ID = KYLIN_CATEGORY_GROUPINGS.SITE_ID "
                +"INNER JOIN KYLIN_ACCOUNT as BUYER_ACCOUNT                                                                                     		"
                +"ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID                                                                                    "
                +"INNER JOIN KYLIN_ACCOUNT as SELLER_ACCOUNT                                                                                    		"
                +"ON KYLIN_SALES.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID                                                                                  "
                +"INNER JOIN KYLIN_COUNTRY as BUYER_COUNTRY                                                                                     		"
                +"ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY                                                                              "
                +"INNER JOIN KYLIN_COUNTRY as SELLER_COUNTRY                                                                                    		"
                +"ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY                                                                            ";

        Dataset<Row>   kylinSalesCube = spark.sql(sql);
        kylinSalesCube.createOrReplaceTempView("kylin_sales_cube");

        Dataset<Row> sqlDF = spark.sql("select part_dt, sum(price) as total_selled, count(distinct seller_id) as sellers from kylin_sales_cube group by part_dt order by part_dt");

//        Dataset<Row> sqlDF = spark.sql("select * from kylin_sales");
        LOG.info("Result total: {}", sqlDF.count());
        sqlDF.show();
    }
}
