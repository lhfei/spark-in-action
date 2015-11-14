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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Apr 6, 2015
 */
public class TwitterStreamingApp {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TwitterStreamingApp")
				.setMaster("local[2]");
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf,
				new Duration(1000));

	}

}
