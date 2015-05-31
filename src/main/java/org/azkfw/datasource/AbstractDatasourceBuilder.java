/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.azkfw.datasource;

import org.azkfw.lang.LoggingObject;

/**
 * このクラスは、データソースをビルドする為の基底クラスです。
 * 
 * @author Kawakicchi
 */
public class AbstractDatasourceBuilder extends LoggingObject {

	/** データソース名 */
	private String name;

	/**
	 * コンストラクタ
	 * 
	 * @param clazz クラス
	 */
	protected AbstractDatasourceBuilder(final Class<?> clazz) {
		super(clazz);
		name = null;
	}

	/**
	 * データソース名を設定する。
	 * 
	 * @param name データソース名
	 */
	public final void setName(final String name) {
		this.name = name;
	}

	/**
	 * データソース名を取得する。
	 * 
	 * @return データソース名
	 */
	public final String getName() {
		return name;
	}
}
