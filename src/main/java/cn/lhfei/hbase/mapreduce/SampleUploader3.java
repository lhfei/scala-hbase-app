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
import java.util.stream.IntStream;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @created Dec 20, 2019
 */
public class SampleUploader3 extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory.getLogger(SampleUploader3.class);
	private static Connection conn;
	private static TableName hTableName;
	private static final String NAME = "SampleUploader";
	private static final String FAMILY_NAME_BASIC = "b";
	private static final String FAMILY_NAME_DETAIL = "d";
	private static final String[] COLUMN_NAMES = {"account_detail_id","member_id","detail_create_date","balance_type","detail_amount","out_trade_no","trade_no","source_id","trade_desc","detail_desc","accountreqcode","trade_type"};
	private static final String[] COLUMN_NAMES_DETAIL = {"customer_id","new_account_no","account_no","account_name","account_balance","detail_modify_date","currency","biz_trade_no","original_trade_no","trade_sub_type","pay_type","trade_date","account_merchan_no","merchant_no","system_source","created_date","modified_date","trade_code","trade_code_name","evidence_no","remark","batch_no","category_code","relation_account_no","exes_type","bussiness_type","original_out_trade_no","card_holder_name","card_no","ext_encrypt","evidence_ext","bill_date","ext_min","ext_mid","ext_max","order_seq_no"};
	
	static class Uploader extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put> {
		private long checkpoint = 100;
		private long count = 0;

		@Override
		public void map(NullWritable key, OrcStruct row, Context context) throws IOException {
			// Input is a ORC file
			// Each map() is a single line, where the key is the line number
			// Each line is comma-delimited; row,family,qualifier,value
			
			System.out.format("ROWKEY = %s_%s_%s", row.getFieldValue("source_id").toString(), row.getFieldValue("detail_create_date").toString(), row.getFieldValue("member_id").toString());
			byte[] rowKey = this.buildRowKey(row.getFieldValue("source_id").toString(), row.getFieldValue("member_id").toString());
			
			// Extract each value

			// Create Put
			Put put = new Put(rowKey);
			
			// build basic info
			IntStream.range(0, COLUMN_NAMES.length).forEach(idx -> {
				String fieldName = COLUMN_NAMES[idx];
				LOG.info("B_FIELD: {}", fieldName);
				String fieldValue = "";;
				if(null != row.getFieldValue(fieldName)) {
					fieldValue = row.getFieldValue(fieldName).toString();
				}
				put.addColumn(FAMILY_NAME_BASIC.getBytes(), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue));
			});
			
			// build detail info
			IntStream.range(0, COLUMN_NAMES_DETAIL.length).forEach(idx -> {
				String fieldName = COLUMN_NAMES_DETAIL[idx];
				LOG.info("B_FIELD: {}", fieldName);
				String fieldValue = "";;
				if(null != row.getFieldValue(fieldName)) {
					fieldValue = row.getFieldValue(fieldName).toString();
				}
				put.addColumn(FAMILY_NAME_DETAIL.getBytes(), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue));
			});

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
		
		private byte[] buildRowKey(String source_id, String member_id) {
			StringBuilder sb = new StringBuilder();
			sb.append(source_id);
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
		job.setInputFormatClass(OrcInputFormat.class);
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
		int status = ToolRunner.run(HBaseConfiguration.create(), new SampleUploader3(), args);
		System.exit(status);
	}
}