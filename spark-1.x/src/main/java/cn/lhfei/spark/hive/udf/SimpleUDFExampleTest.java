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

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Nov 3, 2015
 */
public class SimpleUDFExampleTest {

	@Test
	public void testUDF() {
		SimpleUDFExample example = new SimpleUDFExample();
		Assert.assertEquals("Hello world", example.evaluate(new Text("world")).toString());
	}

	@Test
	public void testUDFNullCheck() {
		SimpleUDFExample example = new SimpleUDFExample();
		Assert.assertNull(example.evaluate(null));
	}
}