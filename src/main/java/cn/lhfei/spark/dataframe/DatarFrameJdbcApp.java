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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Aug 14, 2015
 */

public class DatarFrameJdbcApp {
	private static final Logger log = LoggerFactory.getLogger(DatarFrameJdbcApp.class);
	
	public static void main(String[] args) throws ClassNotFoundException {
		SparkConf sc = new SparkConf().setAppName("DatarFrameJdbcApp").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		SQLContext sqlContext = new SQLContext(jsc);
		
		Class.forName(JdbcUtil.MYSQL_DRIVER_CLASS);
		
		Properties props = new Properties();
		scala.tools.scalap.Properties.setProp("user", "root");
		scala.tools.scalap.Properties.setProp("password", "Ifeng@01");
		
		log.info("=====================================>>>>>>>>>>>>>>> Total: {}", sqlContext.read().jdbc(JdbcUtil.MYSQL_CONNECTION_URL,  "TMP_VDN_LT", props));
	
		Map<String, String> options = new HashMap<>();
		options.put("driver", JdbcUtil.MYSQL_DRIVER_CLASS);
		options.put("url", JdbcUtil.MYSQL_CONNECTION_URL);
		options.put("dbtable", "TMP_FULLY");
		
		DataFrame df = sqlContext.read().options(options).format("jdbc").load();
		JavaRDD<Row> rdd = df.javaRDD();
		
		log.info(">>>>>>>>>>>>>>>>>> Total: {}", rdd.count());
		
		List<Row> list = rdd.filter(new Function<Row, Boolean>() {

			@Override
			public Boolean call(Row row) throws Exception {
				String isp = "";
				isp = row.getString(1);
				log.info("&&&&&&&&&&&&&&&&&&&&&{}", isp);
				return (isp != null && isp.equals("联通"));
			}
			
		}).collect();
		
		log.info("<<<<<<<<<<<<<<<<<<<<<<<< Total: {}", list.size());
		
	}

}
