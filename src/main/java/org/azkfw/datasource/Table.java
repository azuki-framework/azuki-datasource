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

import java.util.List;

/**
 * このインターフェースは、テーブル情報を保持するインターフェースです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/07/31
 * @author Kawakicchi
 */
public interface Table {

	/**
	 * ラベルを取得する。
	 * 
	 * @return ラベル
	 */
	public String getLabel();

	/**
	 * テーブル名を取得する。
	 * 
	 * @return テーブル名
	 */
	public String getName();

	/**
	 * フィールド情報一覧を取得する。
	 * 
	 * @return フィールド情報一覧
	 */
	public List<Field> getFields();

	/**
	 * レコード情報一覧を取得する。
	 * 
	 * @return レコード情報一覧
	 */
	public List<Record> getRecords();
}
