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

package cn.lhfei.hbase.bulk;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 * Spark HBase BulkLoad spark-submit --class cn.lhfei.hbase.bulk.DataImport
 * --master yarn --deploy-mode cluster --driver-memory 512m --executor-cores 1
 * --executor-memory 512m --queue default --verbose tags.jar
 * 
 * @author Hefei Li
 *
 */
public class DataImport {

	private final static String appName = DataImport.class.getSimpleName();
	private final static String HBASE_ZK_PORT_KEY = "hbase.zookeeper.property.clientPort";
	private final static String HBASE_ZK_QUORUM_KEY = "hbase.zookeeper.quorum";
	private final static String HBASE_ZK_PARENT_KEY = "zookeeper.znode.parent";
	private final static String DEFAULT_FS = "fs.defaultFS";
	private final static String DFS_REPLICATION = "dfs.replication";

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.out.println("Usage: required params: <HBaseTable> <Family> <InputDir> <OutputDir>");
			System.exit(-1);
		}

		// HBase Table
		String tableName = args[0];
		// HBase Table Family
		String family = args[1];
		// HBase Table Input RawData
		String inputDir = args[2];
		// HBase Table Input HFileData
		String outputDir = args[3];

		long start = System.currentTimeMillis();
		Configuration hadoopConf = HBaseConfiguration.create();
		hadoopConf.set(DEFAULT_FS, "hdfs://192.168.10.20:8020");
		hadoopConf.set(DFS_REPLICATION, "1");
		hadoopConf.set(HBASE_ZK_PORT_KEY, "2181");
		hadoopConf.set(HBASE_ZK_QUORUM_KEY, "10.220.225.139,10.220.225.138,10.220.225.140");
		hadoopConf.set(HBASE_ZK_PARENT_KEY, "/hbase-unsecure");
		hadoopConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

		Job job = Job.getInstance(hadoopConf, appName);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setOutputFormatClass(HFileOutputFormat2.class);

		FileInputFormat.addInputPaths(job, inputDir);
		FileSystem fs = FileSystem.get(hadoopConf);
		Path output = new Path(outputDir);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		fs.close();
		FileOutputFormat.setOutputPath(job, output);

		Connection connection = ConnectionFactory.createConnection(hadoopConf);
		TableName table = TableName.valueOf(tableName);
		HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(table),
				connection.getRegionLocator(table));

		SparkConf sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.setMaster("local[*]").setAppName(appName);

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		jsc.textFile(inputDir).persist(StorageLevel.MEMORY_AND_DISK_SER())
				.flatMapToPair(line -> extractText(line, family).iterator()).coalesce(1)
				.sortByKey()
				.saveAsNewAPIHadoopFile(outputDir, ImmutableBytesWritable.class, KeyValue.class,
						HFileOutputFormat2.class, job.getConfiguration());

		/*
		 * LoadIncrementalHFiles load = new LoadIncrementalHFiles(hadoopConf);
		 * load.doBulkLoad(output, connection.getAdmin(), connection.getTable(table),
		 * connection.getRegionLocator(table));
		 */

		BulkLoadHFiles blh = BulkLoadHFiles.create(hadoopConf);
		blh.bulkLoad(table, output);

		jsc.close();
	}

	/**
	 * 
	 * @param type
	 *            1User、2Product、3ProductType、4Order、5Log
	 * @param line
	 * @param family
	 * @return
	 */
	static List<Tuple2<ImmutableBytesWritable, KeyValue>> extractText(String line, String family) {
		TreeMap<String, Integer> fieldNames = new TreeMap<String, Integer>() {
			{
				put("id", 0);
				put("name", 1);
				put("level", 2);
				put("pid", 3);
				put("ctime", 4);
				put("utime", 5);
				put("remark", 6);
			}
		};
		List<Tuple2<ImmutableBytesWritable, KeyValue>> arr = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
		String[] fieldValues = line.split("\t", 7);
		if (fieldValues != null && fieldValues.length == 7) {
			String id = fieldValues[0];
			byte[] rowkey = Bytes.toBytes(id);
			byte[] columnFamily = Bytes.toBytes(family);
			ImmutableBytesWritable ibw = new ImmutableBytesWritable(rowkey);
			fieldNames.forEach((k, v) -> {
				arr.add(new Tuple2<>(ibw,
						new KeyValue(rowkey, columnFamily, Bytes.toBytes(k), Bytes.toBytes(fieldValues[v]))));
			});
		}
		return arr;
	}


}