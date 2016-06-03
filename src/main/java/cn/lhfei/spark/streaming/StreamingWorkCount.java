package cn.lhfei.spark.streaming;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lhfei.spark.orm.domain.NginxLog;


public class StreamingWorkCount {
	
	private static final Logger log = LoggerFactory.getLogger(StreamingWorkCount.class);
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		SparkConf sc = new SparkConf().setMaster("local[2]").setAppName("StreamingWorkCount");
	
		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(1));
		
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
		
		JavaDStream<NginxLog> words = lines.map(new Function<String, NginxLog>() {
			@Override
			public NginxLog call(String line) throws Exception {
				log.info(">>>>>>>>>>>>>>>>>Line: [{}]", line);
				String[] items = line.split(" ");
				if(null != items && items.length > 0){
					NginxLog nl = new NginxLog();
					nl.setIp(items[0]);
					nl.setLiveTime(Long.parseLong(items[1]));
					nl.setAgent(items[2]);
					
					return nl;
				}
				return null;
			}
		});
		
		words.print();
		
		jsc.start();
		
		jsc.awaitTermination();
	}

}
