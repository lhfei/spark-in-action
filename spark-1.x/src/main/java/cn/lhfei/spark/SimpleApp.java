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

package cn.lhfei.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Apr 3, 2015
 */
public class SimpleApp {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SimpleApp").setMaster("114.80.177.133[*]");
		
		JavaSparkContext  sc = new JavaSparkContext (conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		
		JavaRDD<Integer> distData = sc.parallelize(data);
		JavaRDD<String> lines = sc.textFile("src/test/resources/data.txt");
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		
	}
}
