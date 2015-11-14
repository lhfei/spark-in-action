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

package cn.lhfei.spark.flume;

import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.SecureRpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Oct 14, 2015
 */

public class FlumeSecureApp {

	public static void main(String[] args) {
		MySecureRpcClientFacade client = new MySecureRpcClientFacade();
		// Initialize client with the remote Flume agent's host, port
		Properties props = new Properties();
		props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, "thrift");
		props.setProperty("hosts", "h1");
		props.setProperty("hosts.h1", "114.80.177.144" + ":" + String.valueOf(12345));

		// Initialize client with the kerberos authentication related properties
		props.setProperty("kerberos", "true");
		props.setProperty("client-principal", "flumeclient/client.example.org@EXAMPLE.ORG");
		props.setProperty("client-keytab", "/tmp/flumeclient.keytab");
		props.setProperty("server-principal", "flume/server.example.org@EXAMPLE.ORG");
		client.init(props);

		// Send 10 events to the remote Flume agent. That agent should be
		// configured to listen with an AvroSource.
		String sampleData = "Hello Flume!";
		for (int i = 0; i < 10; i++) {
			client.sendDataToFlume(sampleData);
		}

		client.cleanUp();
	}
}

class MySecureRpcClientFacade {
	private RpcClient client;
	private Properties properties;

	public void init(Properties properties) {
		// Setup the RPC connection
		this.properties = properties;
		// Create the ThriftSecureRpcClient instance by using
		// SecureRpcClientFactory
		this.client = SecureRpcClientFactory.getThriftInstance(properties);
	}

	public void sendDataToFlume(String data) {
		// Create a Flume Event object that encapsulates the sample data
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

		// Send the event
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			// clean up and recreate the client
			client.close();
			client = null;
			client = SecureRpcClientFactory.getThriftInstance(properties);
		}
	}

	public void cleanUp() {
		// Close the RPC connection
		client.close();
	}

}
