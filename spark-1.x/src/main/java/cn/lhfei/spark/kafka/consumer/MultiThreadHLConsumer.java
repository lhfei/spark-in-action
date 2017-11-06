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

package cn.lhfei.spark.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Sep 1, 2015
 */

public class MultiThreadHLConsumer {
	private ExecutorService executor;
	private final ConsumerConnector consumer;
	private final String topic;

	public MultiThreadHLConsumer(String zookeeper, String groupId, String topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(zookeeper,
						groupId));
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig(String zookeeper,
			String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);

		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public void testMultiThreadConsumer(int threadCount) {
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		// Define thread count for each topic
		topicMap.put(topic, new Integer(threadCount));
		// Here we have used a single topic but we can also add
		// multiple topics to topicCount MAP
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer
				.createMessageStreams(topicMap);
		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap
				.get(topic);
		// Launching the thread pool
		executor = Executors.newFixedThreadPool(threadCount);
		// Creating an object messages consumption
		int count = 0;
		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			final int threadNumber = count;
			executor.submit(new Runnable() {
				public void run() {
					ConsumerIterator<byte[], byte[]> consumerIte = stream
							.iterator();
					while (consumerIte.hasNext())
						System.out.println("Thread Number " + threadNumber
								+ ": "
								+ new String(consumerIte.next().message()));
					System.out.println("Shutting down Thread Number: "
							+ threadNumber);
				}
			});
			count++;
		}
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public static void main(String[] args) {
		String zooKeeper = args[0];
		String groupId = args[1];
		String topic = args[2];
		int threadCount = Integer.parseInt(args[3]);
		MultiThreadHLConsumer multiThreadHLConsumer = new MultiThreadHLConsumer(
				zooKeeper, groupId, topic);
		multiThreadHLConsumer.testMultiThreadConsumer(threadCount);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException ie) {
		}
		multiThreadHLConsumer.shutdown();
	}
}
