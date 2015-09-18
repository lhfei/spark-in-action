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

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Sep 1, 2015
 */

public class SimplePartitioner implements Partitioner {

	public SimplePartitioner(VerifiableProperties props) {
	}

	/*
	 * The method takes the key, which in this case is the IP address, It finds
	 * the last octet and does a modulo operation on the number of partitions
	 * defined within Kafka for the topic.
	 * 
	 * @see kafka.producer.Partitioner#partition(java.lang.Object, int)
	 */
	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String partitionKey = (String) key;
		int offset = partitionKey.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(partitionKey.substring(offset + 1))
					% a_numPartitions;
		}
		return partition;
	}

}
