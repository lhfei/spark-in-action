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

package cn.lhfei.spark.hive.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Nov 3, 2015
 */

public class NameParserGenericUDTFTest {

	@Test
	public void testUDTFOneSpace() {

		// set up the models we need
		NameParserGenericUDTF example = new NameParserGenericUDTF();

		ObjectInspector[] inputOI = { PrimitiveObjectInspectorFactory.javaStringObjectInspector };

		// create the actual UDF arguments
		String name = "John Smith";

		// the value exists
		try {
			example.initialize(inputOI);
		} catch (Exception ex) {

		}

		ArrayList<Object[]> results = example.processInputRecord(name);
		Assert.assertEquals(1, results.size());
		Assert.assertEquals("John", results.get(0)[0]);
		Assert.assertEquals("Smith", results.get(0)[1]);
	}
}
