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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tools.ant.taskdefs.LoadFile;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import cn.lhfei.hbase.config.HBaseProperties;
import cn.lhfei.hbase.util.HashRowKeyGenerator;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @created Dec 20, 2019
 */
public class SampleUploader extends Configured implements Tool {
	private static final String NAME = "SampleUploader";
	private Configuration conf;
	private static HBaseProperties hbaseProperties;
	/*private static final String FAMILY_NAME = "f";
	private static final String[] COLUMN_NAMES = {"create_date", "merchant_no", "member_id", "account_name", "detail_create_date", "out_trade_no", "account_balance", "in_amount", "out_amount", "trade_desc", "bill_date", "ext_min", "order_seq_no"};*/
	private static final HashRowKeyGenerator generator = new HashRowKeyGenerator();
	
	static class Uploader extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		private long checkpoint = 100;
		private long count = 0;

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException {
			// Input is a CSV file
			// Each map() is a single line, where the key is the line number
			// Each line is comma-delimited; row,family,qualifier,value

			// Split CSV line
			String[] values = line.toString().split(",");
			if (values.length != 13) {
				return;
			}
			
//			Table table = conn.getTable(hTableName);
			byte[] rowKey = generator.nextId();

			// Extract each value
			// columns: | create_date | merchant_no | member_id | account_name | detail_create_date | out_trade_no | account_balance | in_amount | out_amount | trade_desc | bill_date | ext_min | order_seq_no |

			// Create Put
			Put put = new Put(rowKey);
			
			hbaseProperties.getColumnsWithFamily().entrySet().forEach(entry -> {
				String[] columns = entry.getValue().split(",");
				IntStream.range(0, columns.length).forEach(idx -> {
					put.addColumn(entry.getKey().getBytes(), Bytes.toBytes(columns[idx]), Bytes.toBytes(values[idx]));
				});
			});
			
			/*IntStream.range(0, COLUMN_NAMES.length).forEach(idx -> {
				put.addColumn(FAMILY_NAME.getBytes(), Bytes.toBytes(COLUMN_NAMES[idx]), Bytes.toBytes(values[idx]));
			});*/

			// Uncomment below to disable WAL. This will improve performance but means
			// you will experience data loss in the case of a RegionServer crash.
			// put.setWriteToWAL(false);

			try {
				context.write(new ImmutableBytesWritable(rowKey), put);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// Set status every checkpoint lines
			if (++count % checkpoint == 0) {
				context.setStatus("Emitting Put " + count);
			}
		}
	}

	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		Path inputPath = new Path(args[0]);
		String tableName = hbaseProperties.getTableName();
		
		Job job = Job.getInstance(conf, NAME + "_" + tableName);
		job.setJarByClass(Uploader.class);
		FileInputFormat.setInputPaths(job, inputPath);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Uploader.class);
		// No reducers. Just write straight to table. Call initTableReducerJob
		// because it sets up the TableOutputFormat.
		TableMapReduceUtil.initTableReducerJob(tableName, null, job);
		job.setNumReduceTasks(0);
		return job;
	}

	/**
	 * Main entry point.
	 *
	 * @param otherArgs
	 *            The command line parameters after ToolRunner handles standard.
	 * @throws Exception
	 *             When running the job fails.
	 */
	public int run(String[] otherArgs) throws Exception {
		if (otherArgs.length != 2) {
			System.err.println("Wrong number of arguments: " + otherArgs.length);
			System.err.println("Usage: " + NAME + " <input> <yaml>");
			return -1;
		}
		Yaml yaml = new Yaml(new Constructor(HBaseProperties.class));
		InputStream in = LoadFile.class.getClassLoader().getResourceAsStream(otherArgs[1]);
		hbaseProperties = yaml.load(in);
		
		conf = getConf();
		conf.set("hbase.zookeeper.quorum", hbaseProperties.getZookeeperQuorum());
		conf.set("zookeeper.znode.parent", hbaseProperties.getZookeeperZnodeParent());		
		
		Job job = configureJob(conf, otherArgs);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(HBaseConfiguration.create(), new SampleUploader(), args);
		System.exit(status);
	}
}