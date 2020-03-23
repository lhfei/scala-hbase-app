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

package cn.lhfei.hbase.mapreduce;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @created Mar 17, 2020
 */

public class SparkBulkLoader {

	public static void main(String[] args) {
		String family = "";
		
		SparkSession spark = SparkSession
			      .builder()
			      .appName("JavaWordCount")
			      .getOrCreate();
		
		Dataset<Row> df = spark.read().orc("/warehouse/tablespace/managed/hive/benchmark.db/odm_pay_merchant_account_details_x_i_d/dt=2020-03-14/000000_0_copy_15");
		
		// Register the Dataframe as a SQL temporary view.
		df.createOrReplaceTempView("DS");
		
		Dataset<Row> sqlDF = spark.sql("select * from DS limit 10");
		
		sqlDF.show();
	}

}
