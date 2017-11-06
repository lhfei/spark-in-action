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
package cn.lhfei.spark.df.streaming;

import cn.lhfei.spark.df.streaming.vlog.VType;
import cn.lhfei.spark.df.streaming.vo.VLogger;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Aug 15, 2016
 */
public class VLogDriver {

	static SimpleDateFormat SF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static final String NORMAL_DATE = "1976-01-01";

	public static void main(String[] args) {
		JavaSparkContext jsc = null;
		SparkConf conf = new SparkConf().setMaster("local").setAppName("VLogger Parser");
		jsc = new JavaSparkContext(conf);
		
		SparkSession spark = SparkSession.builder().appName("VLogger Parser").getOrCreate();

		JavaRDD<VLogger> records = filteLog(jsc);
		
		JavaPairRDD<String, Iterable<VLogger>> groups = reduceBy(records);
		
		Map<String, Long> keyMap = groups.countByKey();
		
		Iterator<String> keys = keyMap.keySet().iterator();
		
		System.out.println("--------------------------------");
		while(keys.hasNext()){
			String key = keys.next();
			System.out.format("Key: [%s], Total: [%d]", key, keyMap.get(key));
			System.out.println();
		}
		System.out.println("--------------------------------");
		
		analysis(spark, records);
	}
	
	public static void analysis(SparkSession spark, JavaRDD<VLogger> records) {
		Dataset<Row> df = spark.createDataFrame(records, VLogger.class);
		
		df.createOrReplaceTempView("VL");

		df.show();
		
		spark.sql("select * from VL limit 5").show();
		
	}
	
	public static JavaPairRDD<String, Iterable<VLogger>> reduceBy(JavaRDD<VLogger> records) {
		JavaPairRDD<String, Iterable<VLogger>> result = records.groupBy(new Function<VLogger, String>() {

			@Override
			public String call(VLogger v1) throws Exception {
				if(null != v1){
					return v1.getCat();
				}else 
					return "#";
			}
			
		});
		
		return result;
	}

	public static JavaRDD<VLogger> filteLog(JavaSparkContext jsc) {

		JavaRDD<String> lines = jsc.textFile("/spark-data/vlogs/0001.txt");

		System.out.println("====== Line count: " + lines.count());

		JavaRDD<VLogger> records = lines.map(new Function<String, VLogger>() {

			TimeStamp ts = null;

			@Override
			public VLogger call(String line) throws Exception {
				VLogger vlog = null;
				String separator = " ";
				String[] items = line.split(separator);
				
				//System.out.format("<<<<<<<Items length: [%d]", items.length);

				String err = "";
				if (items.length == 24) {
					vlog = new VLogger();
					ts = new TimeStamp(Long.parseLong(items[11]));
					
					err = items[15];
					if (err != null && err.startsWith("301030_")) {
						err = "301030_X";
					}
					
					vlog.setErr(err);
					vlog.setIp(items[2]);
					vlog.setRef(items[3]);
					vlog.setSid(items[4]);
					vlog.setUid(items[5]);
					vlog.setLoc(items[8]);
					vlog.setCat(VType.getVType(items[21], items[7], items[20]));
					
					try {
						vlog.setTm(SF.format(ts));
					} catch (Exception e) {
						vlog.setTm(NORMAL_DATE);
					}
					
					vlog.setUrl(items[12]);
					vlog.setDur(items[14]);
					vlog.setBt(items[16]);
					vlog.setBl(items[17]);
					vlog.setLt(items[18]);
					vlog.setVid(items[20]);
					vlog.setPtype(items[21]);
					vlog.setCdnId(items[22]);
					vlog.setNetname(items[23]);

				}

				return vlog;
			}

		});
		
		records.filter(new Function<VLogger, Boolean>() {
			@Override
			public Boolean call(VLogger v1) throws Exception {
				
				return null != v1;
			}
		});
		
		return records;
	}
}
