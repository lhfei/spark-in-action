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

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lhfei.spark.AbstractSparkApp;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Aug 24, 2015
 */

public class FirstRDDDriver extends AbstractSparkApp {
	private static final Logger log = LoggerFactory.getLogger(FirstRDDDriver.class);
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("FirstRDD");
		SparkContext sc = new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		JavaRDD<Integer> nums = jsc.parallelize(Arrays.asList(1, 2, 3, 4 , 5, 6, 7, 8, 9)).cache();
		
		JavaRDD<Integer> even;
		even = nums.filter(new Function<Integer, Boolean>() {
			private static final long serialVersionUID = 3721385582100829201L;

			@Override
			public Boolean call(Integer v) throws Exception {
				return v % 2 == 0;
			}
		}).cache();

		List<Integer> result = even.collect();
		for(Integer val : result) {
			log.info("Even Number: >>>>>>>>>>>>>>>>>>>>>>.{}", val);
		}
		
		JavaRDD<Integer> odd = nums.subtract(even);
		
		for(Integer val : odd.collect()) {
			log.info("Odd Number: >>>>>>>>>>>>>>>>>>>>>>.{}", val);
		}
	}

}
