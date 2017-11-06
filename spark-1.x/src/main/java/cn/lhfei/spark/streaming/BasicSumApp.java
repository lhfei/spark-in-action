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
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Apr 9, 2015
 */

public class BasicSumApp  {
	
	protected static Logger log = LoggerFactory.getLogger(BasicSumApp.class);

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("BasicSumApp");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaDoubleRDD factors = sc.parallelizeDoubles(Arrays.asList(2.2d, 789.78d, 98.99d, 0.87d, 184.789d));
		
		Double result = factors.fold(0.0d, new Function2<Double, Double, Double>() {

			@Override
			public Double call(Double v1, Double v2) throws Exception {
				
				return v1 + v2;
			}
			
		});
		
		log.info("Result: {}" ,result);
		
	}

}
