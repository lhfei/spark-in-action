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

package cn.lhfei.spark.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
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
public class ComplexUDFExampleTest {

	@Test
	public void testComplexUDFReturnsCorrectValues() throws HiveException {

		// set up the models we need
		ComplexUDFExample example = new ComplexUDFExample();
		ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
		JavaBooleanObjectInspector resultInspector = (JavaBooleanObjectInspector) example
				.initialize(new ObjectInspector[] { listOI, stringOI });

		// create the actual UDF arguments
		List<String> list = new ArrayList<String>();
		list.add("a");
		list.add("b");
		list.add("c");

		// test our results

		// the value exists
		Object result = example
				.evaluate(new DeferredObject[] { new DeferredJavaObject(list), new DeferredJavaObject("a") });
		Assert.assertEquals(true, resultInspector.get(result));

		// the value doesn't exist
		Object result2 = example
				.evaluate(new DeferredObject[] { new DeferredJavaObject(list), new DeferredJavaObject("d") });
		Assert.assertEquals(false, resultInspector.get(result2));

		// arguments are null
		Object result3 = example
				.evaluate(new DeferredObject[] { new DeferredJavaObject(null), new DeferredJavaObject(null) });
		Assert.assertNull(result3);
	}
}
