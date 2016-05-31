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

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lhfei.spark.orm.domain.NginxLog;
import scala.Tuple2;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Apr 14, 2015
 */

public class NginxlogSorter {
	protected static Logger log = LoggerFactory.getLogger(NginxlogSorter.class);
	
	public static void main(String[] args) {
		JavaSparkContext sc = null;
		try {
			SparkConf conf = new SparkConf().setMaster("local").setAppName("NginxlogSorter");
			//conf.set("hadoop.home.dir", "/usr/hdp/2.4.0.0-169/hadoop");
			
			sc = new JavaSparkContext(conf);
			
			
			JavaRDD<String> lines = sc.textFile("src/test/resources/nginx_report.txt");
			
			JavaRDD<NginxLog> items = lines.map(new Function<String, NginxLog>(){
				private static final long serialVersionUID = -1530783780334450383L;

				@Override
				public NginxLog call(String v1) throws Exception {
					NginxLog item = new NginxLog();
					String[] arrays = v1.split("[\\t]");
					
					if(arrays.length == 3){
						item.setIp(arrays[0]);
						item.setLiveTime(Long.parseLong(arrays[1]));
						item.setAgent(arrays[2]);
					}
					return item;
				}
			});
			
			log.info("=================================Length: [{}]", items.count());
			
			JavaPairRDD<String, Iterable<NginxLog>> keyMaps = items.groupBy(new Function<NginxLog, String>(){

				@Override
				public String call(NginxLog v1) throws Exception {
					return v1.getIp();
				}
			});
			
			log.info("=================================Group by Key Length: [{}]", keyMaps.count());
			
			keyMaps.foreach(new VoidFunction<Tuple2<String, Iterable<NginxLog>>>(){

				@Override
				public void call(Tuple2<String, Iterable<NginxLog>> t) throws Exception {
					log.info("++++++++++++++++++++++++++++++++ key: {}", t._1);
					
					Iterator<NginxLog> ts = t._2().iterator();
					
					while(ts.hasNext()){
						log.info("=====================================[{}]",ts.next().toString());
					}
				}
				
			});
			
			keyMaps.saveAsTextFile("src/test/resources/nginx_report-result.txt");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sc.close();
		}
	}

}
