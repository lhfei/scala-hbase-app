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

package cn.lhfei.hbase.config;

import java.util.Map;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 *         created Mar 31, 2020
 */
public class HBaseProperties {

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getZookeeperQuorum() {
		return zookeeperQuorum;
	}

	public void setZookeeperQuorum(String zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}

	public String getZookeeperZnodeParent() {
		return zookeeperZnodeParent;
	}

	public void setZookeeperZnodeParent(String zookeeperZnodeParent) {
		this.zookeeperZnodeParent = zookeeperZnodeParent;
	}

	public Map<String, String> getColumnsWithFamily() {
		return columnsWithFamily;
	}

	public void setColumnsWithFamily(Map<String, String> columnsWithFamily) {
		this.columnsWithFamily = columnsWithFamily;
	}

	private String tableName;
	private String zookeeperQuorum;
	private String zookeeperZnodeParent;
	private Map<String, String> columnsWithFamily;
}
