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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Apr 7, 2015
 */
public class NetworkWordCount {

	private static final Logger log = LoggerFactory
			.getLogger(NetworkWordCount.class);

	public static void main(String[] args) {
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
			@Override
			public Iterable<String> call(String t) throws Exception {

				return Arrays.asList(t.split(" "));
			}
			
		});
		
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer> (){
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s, 1);
			}
			
		});
		
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2)
					throws Exception {
				return i1 + i2;
			}
		});
		
		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate

	}
}
