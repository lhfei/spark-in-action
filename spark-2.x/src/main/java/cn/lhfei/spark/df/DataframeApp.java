package cn.lhfei.spark.df;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Aug 13, 2016
 */
public class DataframeApp {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.master("local")
				.appName("Spark Dataframes, Dataset and SQL.")
				.getOrCreate();
		
		Dataset<Row> df = spark.read().json("/spark-data/test/DataSource.json");
		
		df.show();
		
		// Register the Dataframe as a SQL temporary view.
		df.createOrReplaceTempView("DS");
		
		Dataset<Row> sqlDF = spark.sql("select * from DS");
		
		sqlDF.show();
		

	}

}
