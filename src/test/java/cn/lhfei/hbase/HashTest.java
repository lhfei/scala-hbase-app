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

package cn.lhfei.hbase;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @created Dec 24, 2019
 */

public class HashTest {
	private static final Logger LOG = LoggerFactory.getLogger(HashTest.class);
	private long currentId = 1;
    private long currentTime = System.currentTimeMillis();

    private Random random = new Random();
    
	@Test
	public void get() {
		
		String key = "2vc6a1c288efa5-777c-4986-a541-7fa2000393cd";
		
		LOG.info("{}", key.hashCode());
	}
	
	@Test
	public void md() {
		try {
			currentTime += random.nextInt(1000);

			byte[] lowT = Bytes.copy(Bytes.toBytes(currentTime), 4, 4);
			byte[] lowU = Bytes.copy(Bytes.toBytes(currentId), 4, 4);

			byte[] rowkey =  Bytes.add(MD5Hash.getMD5AsHex(Bytes.add(lowU, lowT)).substring(0, 8).getBytes(),
					Bytes.toBytes(currentId));
			
			LOG.info("rowkey = {}", Bytes.toString(rowkey));
		} finally {
			currentId++;
		}
	}
	@Test
	public void md5() {
		try {
			String key = "2vc6a1c288efa5-777c-4986-a541-7fa2000393cd";
			MessageDigest md = MessageDigest.getInstance("MD5");
			StringBuilder rowKey = new StringBuilder();
		
			rowKey.append(MD5Hash.getMD5AsHex(Bytes.toBytes(key)));
			
			rowKey.append("|");
			
			rowKey.append(md.digest(key.getBytes()));
			
			
			LOG.info("rowkey = {}", String.valueOf(rowKey.toString()));
			
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} finally {
			currentId++;
		}
	}
	
	@Test
	public void getDt() {
		String[] names = {"account_detail_id", "member_id", "detail_create_date", "balance_type", "detail_amount", "out_trade_no", "trade_no", "source_id", "trade_desc", "detail_desc", "accountreqcode", "trade_type"};
		String dt = "2019-01-31 07:25:21.333234";
		String line = "24554180140,110046586003,2019-01-31 07:25:21.333234,OUT,250210.0,JDDORS_10000032292412_31,2019013120004100014831269970,201901313000493377313,POP-应收应付款 - (JDDORS_10000032292412_31)10000032292412@POP结算,转账,20190131022007912515489195219533,T_AGD";
		String[] cls = line.split(",");
		
		if(cls.length == 11) {
			cls[11] = "";
		}
		LOG.info("====={}", cls.length);
		IntStream.range(0, cls.length).forEach(idx -> {
			LOG.info("Column[{}-{}]: {}", idx, names[idx], cls[idx]);
		});
		
		LOG.info(dt.substring(0, 10));
	}
	
	public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 16);
		BigInteger highestKey = new BigInteger(endKey, 16);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
			byte[] b = String.format("%016x", key).getBytes();
			splits[i] = b;
		}
		return splits;
	}

}
