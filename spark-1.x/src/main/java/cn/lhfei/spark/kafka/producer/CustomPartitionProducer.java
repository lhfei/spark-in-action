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

package cn.lhfei.spark.kafka.producer;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Sep 1, 2015
 */

public class CustomPartitionProducer {
	private static final Logger log = LoggerFactory.getLogger(CustomPartitionProducer.class);
	
	private static Producer<String, String> producer;

	public CustomPartitionProducer() {
		Properties props = new Properties();
		// Set the broker list for requesting metadata to find the lead broker
		props.put("metadata.broker.list",
				"192.168.146.132:9092, 192.168.146.132:9093, 192.168.146.132:9094");
		// This specifies the serializer class for keys
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// Defines the class to be used for determining the partition
		// in the topic where the message needs to be sent.
		props.put("partitioner.class", "kafka.examples.ch4.SimplePartitioner");
		// 1 means the producer receives an acknowledgment once the lead replica

		// has received the data. This option provides better durability as the
		// client waits until the server acknowledges the request as successful.

		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	public static void main(String[] args) {
		int argsCount = args.length;
		if (argsCount == 0 || argsCount == 1)
			throw new IllegalArgumentException(
					"Please provide topic name and Message count as arguments");
		// Topic name and the message count to be published is passed from the
		// command line
		String topic = (String) args[0];
		String count = (String) args[1];
		int messageCount = Integer.parseInt(count);

		log.info("Topic Name - {}" ,topic);
		log.info("Message Count - {}" ,messageCount);
		CustomPartitionProducer simpleProducer = new CustomPartitionProducer();
		simpleProducer.publishMessage(topic, messageCount);
	}

	private void publishMessage(String topic, int messageCount) {
		Random random = new Random();
		for (int mCount = 0; mCount < messageCount; mCount++) {
			String clientIP = "192.168.14." + random.nextInt(255);
			String accessTime = new Date().toString();
			String message = accessTime + ",kafka.apache.org," + clientIP;
			log.info(message);
			// Creates a KeyedMessage instance
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					topic, clientIP, message);
			// Publish the message
			producer.send(data);
		}
		// Close producer connection with broker.
		producer.close();
	}
}
