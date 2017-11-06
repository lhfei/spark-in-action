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

package cn.lhfei.spark.dataframe;

import cn.lhfei.spark.orm.domain.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Aug 14, 2015
 */

public class DataFrameApp {

	private static final Logger log = LoggerFactory.getLogger(DataFrameApp.class);
	
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setMaster("local").setAppName("DataFrameApp");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		SQLContext sqlContext = new SQLContext(jsc);
		
		DataFrame df = sqlContext.read().json("src/test/resources/people.json");
		/*df.show();
		
		df.select(df.col("name"), df.col("age").plus(1)).show();
		
		df.filter(df.col("age").gt(18)).show();*/
		
		
		JavaRDD<Person> people = jsc.textFile("src/test/resources/people.txt").map(new Function<String, Person>(){
			@Override
			public Person call(String line) throws Exception {
				String[] items = line.split(",");
				Person person = new Person();
				person.setName(items[0]);
				person.setAge(Integer.parseInt(items[1].trim()));
				return person;
			}
		});
		
		// Apply a schema to an RDD of JavaBeans and register it as a table.
		DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
		schemaPeople.registerTempTable("PEOPLE");
		
		DataFrame teenagers  = sqlContext.sql("SELECT name, age from PEOPLE WHERE age > 18");
		
		
		List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {

			@Override
			public String call(Row row) throws Exception {
				log.info(row.toString());
				
				return row.getString(0);
			}
			
		}).collect();
		
	}

}
