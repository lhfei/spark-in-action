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
package cn.lhfei.spark.df.streaming.adult;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Sep 2, 2016
 * 
 * useage: ./bin/spark-submit --class cn.lhfei.spark.df.streaming.adult.AdultDriver --master local[1] /home/lhfei/spark_jobs/spark-2.x-1.0.0.jar
 */
public class AdultDriver {
	
	private static final Logger log = LoggerFactory.getLogger(AdultDriver.class);

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Adult Data Set")
				.getOrCreate();
		
		
		Dataset<String> ds = spark.read().textFile("/spark-data/adult.data");
		
		JavaRDD<Adult> records = ds.javaRDD().map(new Function<String, Adult>() {

			@Override
			public Adult call(String line) throws Exception {
				String[] items = line.split(", ");
				if(items.length != 15){
					return null;
				}
				int age;
				int fnlwgt;
				int education_num;
				int capital_gain;
				int capital_loss;
				int hours_per_week;
				
				try {
					age = Integer.parseInt(items[0]);
				} catch (Exception e) {
					age = -1;
				}
				try {
					fnlwgt = Integer.parseInt(items[2]);
				} catch (Exception e) {
					fnlwgt = -1;
				}
				try {
					education_num = Integer.parseInt(items[4]);
				} catch (Exception e) {
					education_num = -1;
				}
				try {
					capital_gain = Integer.parseInt(items[10]);
				} catch (Exception e) {
					capital_gain = -1;
				}
				try {
					capital_loss = Integer.parseInt(items[11]);
				} catch (Exception e) {
					capital_loss = -1;
				}
				try {
					hours_per_week = Integer.parseInt(items[12]);
				} catch (Exception e) {
					hours_per_week = -1;
				}
					
				Adult adult = new Adult(
						age,
						items[1 ],
						fnlwgt,
						items[3 ],
						education_num,
						items[5 ],
						items[6 ],
						items[7 ],
						items[8 ],
						items[9 ],
						capital_gain,
						capital_loss,
						hours_per_week,
						items[13],
						items[14]						
						);
				
				return adult;
			}
		}).filter(new Function<Adult, Boolean>() {

			@Override
			public Boolean call(Adult adult) throws Exception {
				return adult != null;
			}
			
		});
		
		
		Dataset<Row> df = spark.createDataFrame(records, Adult.class);
		
		df.createOrReplaceTempView("adult");
		
		Dataset<Row> highIncome = spark.sql("select * from adult where income = '>50K'");
		Dataset<Row> lowIncome = spark.sql("select * from adult where income = '<=50K'");
		
		log.info("Higth income count: [{}],  Low income count: [{}]", highIncome.count(), lowIncome.count());
		
		highIncome.show();
		lowIncome.show();
	}
}
