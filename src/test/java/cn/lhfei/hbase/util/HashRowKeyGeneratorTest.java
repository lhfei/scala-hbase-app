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

package cn.lhfei.hbase.util;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @created Dec 25, 2019
 */
public class HashRowKeyGeneratorTest {
	private static final Logger LOG = LoggerFactory.getLogger(HashRowKeyGeneratorTest.class);
	
	@Test
	public void get() {
		String key = "2vc6a1c288efa5-777c-4986-a541-7fa2000393cd";
		HashRowKeyGenerator gen = new HashRowKeyGenerator();
		
		LOG.info("{}", Bytes.toString(HashRowKeyGenerator.getNumRowkey(key, 10)));
//		IntStream.range(0, 100).forEach(idx -> {
//			LOG.info("Row key = {}", Arrays.toString(gen.nextId()));
//		});
	}
	
	@Test
	public void getKey() {
		String key = "2vc6a1c288efa5-777c-4986-a541-7fa2000393cd";
		HashRowKeyGenerator gen = new HashRowKeyGenerator();
		IntStream.range(0, 100).forEach(idx -> {
			LOG.info("{}", Bytes.toString(gen.nextId()));
		});
	}
}
