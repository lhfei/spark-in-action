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

package cn.lhfei.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Apr 14, 2015
 */

public class NginxlogSorter {
	protected static Logger log = LoggerFactory.getLogger(BasicSumApp.class);
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("NginxlogSorter");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		JavaRDD<String> lines = sc.textFile("src/test/resources/nginx_report.txt");
		
		JavaPairRDD<String, Integer> items = lines.mapToPair(new PairFunction<String, String, Integer>(){

			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				log.info(s);
				return null;
			}
			
		});
		
		lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){

			@Override
			public Iterable<Tuple2<String, Integer>> call(String t)
					throws Exception {
				
				log.info(">>>: {}", t);
				return null;
			}
			
		});
	}

}
