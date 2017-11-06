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

package cn.lhfei.spark.rdd;

import cn.lhfei.spark.AbstractSparkApp;
import cn.lhfei.spark.rdd.tool.VideologFilter;
import cn.lhfei.spark.rdd.tool.VideologPair;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Iterator;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Aug 25, 2015
 */

public class PairRDDDriver extends AbstractSparkApp{
	private static final Logger log = LoggerFactory.getLogger(PairRDDDriver.class);
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("PairRDD");
		SparkContext sc = new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);

		JavaRDD<String> ipRepo = jsc.textFile("src/test/resources/data/IP_REPO_CN.txt").cache();
		JavaRDD<String> logs = jsc.textFile("src/test/resources/data/IP_LOGS.log").cache();
		
		JavaPairRDD<String, String> ipPair = ipRepo.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				//log.info("====================={}", line);
				return new Tuple2(line.split("\t")[0], line);
			}
		}).distinct().cache();
		
		JavaPairRDD<String, String> errPair = logs.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				//log.info("====================={}", line);
				VideologPair pair = VideologFilter.filte(line, "2015-07-02", "0000");
				return new Tuple2(pair.getKey(), pair.getValue());
			}
		}).cache();
		
		log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Key Size is: {}", errPair.keys().count());
		
		errPair.groupByKey().map(new Function<Tuple2<String, Iterable<String>>, String>() {

			@Override
			public String call(Tuple2<String, Iterable<String>> val)
					throws Exception {
				
				Iterator<String> iter = val._2().iterator();
				while(iter.hasNext()) {
					log.info("Key:{}: Value: {}", val._1, iter.next());
				}
				
				return val._1();
			}
			
		}).count();
		
		
		errPair.count();
		ipPair.count();

		
		jsc.close();
		
	}

}
