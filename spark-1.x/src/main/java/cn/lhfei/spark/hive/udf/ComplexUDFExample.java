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

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Nov 3, 2015
 */

class ComplexUDFExample extends GenericUDF {

	ListObjectInspector listOI;
	StringObjectInspector elementOI;

	@Override
	public String getDisplayString(String[] arg0) {
		return "arrayContainsExample()"; // this should probably be better
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
		}
		// 1. Check we received the right object types.
		ObjectInspector a = arguments[0];
		ObjectInspector b = arguments[1];
		if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
			throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
		}
		this.listOI = (ListObjectInspector) a;
		this.elementOI = (StringObjectInspector) b;

		// 2. Check that the list contains strings
		if (!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
			throw new UDFArgumentException("first argument must be a list of strings");
		}

		// the return type of our function is a boolean, so we provide the
		// correct object inspector
		return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		// get the list and string from the deferred objects using the object
		// inspectors
		List<String> list = (List<String>) this.listOI.getList(arguments[0].get());
		String arg = elementOI.getPrimitiveJavaObject(arguments[1].get());

		// check for nulls
		if (list == null || arg == null) {
			return null;
		}

		// see if our list contains the value we need
		for (String s : list) {
			if (arg.equals(s))
				return new Boolean(true);
		}
		return new Boolean(false);
	}

}
