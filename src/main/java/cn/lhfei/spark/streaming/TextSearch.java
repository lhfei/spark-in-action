package cn.lhfei.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextSearch {
	private static final Logger log = LoggerFactory.getLogger(TextSearch.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TextSearch").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("src/test/resources/data.txt");
		
		JavaRDD<String> keys = lines.filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String v1) throws Exception {

				return v1.contains("Hello");
			}
		}).cache();
		
		log.info("Result:  [Hello]--{}", keys.count());
		
		keys.collect();
		
		keys.saveAsTextFile("src/test/resources/keywords.txt");
	}

}
