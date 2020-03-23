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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jodd.util.StringUtil;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @created Dec 20, 2019
 */
public class SampleUploader2 extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory.getLogger(SampleUploader2.class);
//	private static Configuration config;
	private static Connection conn;
	private static TableName hTableName;

	/*static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "172.23.226.122,172.23.226.209,172.23.227.70");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
	}*/
	private static final String NAME = "SampleUploader";
	private static final String FAMILY_NAME_BASIC = "b";
	private static final String FAMILY_NAME_DETAIL = "d";
//	private static final String[] COLUMN_NAMES = {"create_date", "merchant_no", "member_id", "account_name", "detail_create_date", "out_trade_no", "account_balance", "in_amount", "out_amount", "trade_desc", "bill_date", "ext_min", "order_seq_no"};
	private static final String[] COLUMN_NAMES = {"account_detail_id", "member_id", "detail_create_date", "balance_type", "detail_amount", "out_trade_no", "trade_no", "source_id", "trade_desc", "detail_desc", "accountreqcode", "trade_type"};
	
//	private static final HashRowKeyGenerator generator = new HashRowKeyGenerator();
	
	static class Uploader extends Mapper<LongWritable, OrcStruct, ImmutableBytesWritable, Put> {
		private long checkpoint = 100;
		private long count = 0;

		@Override
		public void map(LongWritable key, OrcStruct line, Context context) throws IOException {
			// Input is a CSV file
			// Each map() is a single line, where the key is the line number
			// Each line is comma-delimited; row,family,qualifier,value
			String content = line.toString();
			LOG.info("{}", content);
			
			// Split CSV line
			String[] values = content.split(",");
//			if (values.length != 13) {
//				return;
//			}
			if (StringUtils.isEmpty(values[1]) // `member_id` is empty
					|| StringUtil.isEmpty(values[2])	//  `detail_create_date`
					|| StringUtil.isEmpty(values[7])) { // `source_id`
				return ;
			}
			
//			Table table = conn.getTable(hTableName);
//			byte[] rowKey = generator.nextId();
			// rowkey = source_id_{detail_create_date.substring(0,10)}_member_id
			byte[] rowKey = this.buildRowKey(values[7], values[2], values[1]);

			// Extract each value
			// columns: | create_date | merchant_no | member_id | account_name | detail_create_date | out_trade_no | account_balance | in_amount | out_amount | trade_desc | bill_date | ext_min | order_seq_no |
			// columns: | account_detail_id | member_id | detail_create_date | balance_type | detail_amount | out_trade_no | trade_no | source_id | trade_desc | detail_desc | accountreqcode | trade_type |

			// Create Put
			Put put = new Put(rowKey);
			
			// build basic info
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("account_detail_id"), Bytes.toBytes(values[0]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("member_id"), Bytes.toBytes(values[1]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("detail_create_date"), Bytes.toBytes(values[2]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("balance_type"), Bytes.toBytes(values[3]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("detail_amount"), Bytes.toBytes(values[4]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("out_trade_no"), Bytes.toBytes(values[5]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("trade_no"), Bytes.toBytes(values[6]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("source_id"), Bytes.toBytes(values[7]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("trade_desc"), Bytes.toBytes(values[8]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("detail_desc"), Bytes.toBytes(values[9]));
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("accountreqcode"), Bytes.toBytes(values[10]));
			
			put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("accountreqcode"), Bytes.toBytes(values[10]));
			
			if(values.length == 11) {
				put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("trade_type"), Bytes.toBytes(""));
			}else {
				put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes("trade_type"), Bytes.toBytes(values[11]));
			}
			
			// build detail info
			
			
//			IntStream.range(0, COLUMN_NAMES.length).forEach(idx -> {
//				put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes(COLUMN_NAMES[idx]), Bytes.toBytes(values[idx]));
//			});

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
		
		private byte[] buildRowKey(String source_id, String detail_cretae_date, String member_id) {
			StringBuilder sb = new StringBuilder();
			sb.append(source_id);
			sb.append("_");
			
			sb.append(detail_cretae_date.substring(0, 10));
			sb.append("_");
			
			sb.append(member_id);

			return Bytes.toBytes(sb.toString());
		}
	}

	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		JobConf jconf = new JobConf(conf);
		Path inputPath = new Path(args[0]);
		String tableName = args[1];
		
		jconf.set("hbase.zookeeper.quorum", "10.220.225.139,10.220.225.138,10.220.225.140");
		jconf.set("zookeeper.znode.parent", "/hbase-unsecure");
		
		Job job = Job.getInstance(jconf, NAME + "_" + tableName);
		job.setJarByClass(Uploader.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileInputFormat.addInputPath(job, inputPath);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(OrcNewInputFormat.class);
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
			System.err.println("Usage: " + NAME + " <input> <tablename>");
			return -1;
		}
		hTableName = TableName.valueOf(otherArgs[1]);
		Job job = configureJob(getConf(), otherArgs);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(HBaseConfiguration.create(), new SampleUploader2(), args);
		System.exit(status);
	}
}