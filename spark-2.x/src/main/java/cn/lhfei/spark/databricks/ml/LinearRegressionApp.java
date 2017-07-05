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
package cn.lhfei.spark.databricks.ml;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Dec 21, 2016
 */
public class LinearRegressionApp {

	private static final Logger log = LoggerFactory.getLogger(LinearRegressionApp.class);
	private static final Marker marker = MarkerFactory.getMarker("DB-");
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.master("local")
				.appName("Linear Regression with Single Variable")
				.getOrCreate();
		
		
		Dataset<Row> training = spark.read().format("com.databricks.spark.csv")
			.option("header", true).option("inferSchema", "true")
			.load("spark-2.x/src/test/databricks-datasets/samples/population-vs-price/data_geo.csv");
		
		training.cache();
		
		
		Long total = training.count();
		
		
		log.info(marker, "==== Count: {}", total);
		
		
		training.printSchema();
		
		/*
		 * This will let us access the table from our SQL notebook! Note - we're
		 * using `createOrReplaceTempView` instead of `registerTempTable`
		 */
		training.createOrReplaceTempView("data_geo");
		
		Dataset<Row> data = spark.sql("select `2014 Population estimate`, `2015 median sales price` as label from data_geo");
		data.printSchema();
		data.show();
		
		VectorAssembler assembler = new org.apache.spark.ml.feature.VectorAssembler();
		
		assembler.setInputCols(new String[]{"2014 Population estimate"});
		assembler.setOutputCol("features");
		
		Dataset<Row> output = assembler.transform(data);
		output.show();
		
		LinearRegression lrA = new LinearRegression().setRegParam(0.0D);
		LinearRegression lrB = new LinearRegression().setRegParam(100.0D);
		
		LinearRegressionModel modelA = lrA.fit(output);
		LinearRegressionModel modelB = lrB.fit(output);
		
		log.info(">>>> ModelA intercept: {}, coefficient: {}" ,modelA.intercept(), modelA.coefficients());
		log.info(">>>> ModelA intercept: {}, coefficient: {}" ,modelB.intercept(), modelB.coefficients());
		
		// Make predictions
		Dataset<Row> predictionA = modelA.transform(output);
		predictionA.show();

		RegressionEvaluator evaluator = new RegressionEvaluator();
		evaluator.setMetricName("rmse");
		
		double rmse = evaluator.evaluate(predictionA);
		log.info("ModelA: Root Mean Squared Error = {}", rmse);
		
		

	    // Summarize the model over the training set and print out some metrics.
	    LinearRegressionTrainingSummary trainingSummary = modelA.summary();
	    log.info("numIterations: {}", trainingSummary.totalIterations());
	    log.info("objectiveHistory: {}", Vectors.dense(trainingSummary.objectiveHistory()));
	    trainingSummary.residuals().show();
	    log.info("RMSE: {}", trainingSummary.rootMeanSquaredError());
	    log.info("r2: {}", trainingSummary.r2());
		
		
	}

}
