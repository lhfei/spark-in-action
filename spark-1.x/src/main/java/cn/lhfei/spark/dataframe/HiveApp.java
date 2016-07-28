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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Aug 17, 2015
 */

public class HiveApp {
	private static final Logger log = LoggerFactory.getLogger(HiveApp.class);

	public static void main(String[] args) throws ClassNotFoundException {
		SparkConf sc = new SparkConf().setMaster("local").setAppName("HiveApp");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(jsc.sc());
		
		sqlContext.sql("");
	}

}
