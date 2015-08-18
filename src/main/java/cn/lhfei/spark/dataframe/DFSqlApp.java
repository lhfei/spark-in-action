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

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Aug 17, 2015
 */

public class DFSqlApp {
	private static final Logger log = LoggerFactory.getLogger(DFSqlApp.class);

	public static void main(String[] args) throws ClassNotFoundException {
		SparkConf sc = new SparkConf().setMaster("local").setAppName("DFSqlApp");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		SQLContext sqlContext = new SQLContext(jsc);
		
		Properties props = new Properties();
		props.setProperty("driver", JdbcUtil.MYSQL_DRIVER_CLASS);
		props.setProperty("url", JdbcUtil.MYSQL_CONNECTION_URL);
		props.setProperty("user", "root");
		props.setProperty("password", "Ifeng@01");
		
		sqlContext.setConf(props);
		
		Class.forName(JdbcUtil.MYSQL_DRIVER_CLASS);
		
		sqlContext.sql("SELECT * FROM TMP_FULLY WHERE isp='联通' ");
		

		
		
	}

}
