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
package org.azkfw.datasource.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.azkfw.datasource.Field;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;

/**
 * このクラスは、データソース関連のユーティリティを提供するクラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/08/01
 * @author Kawakicchi
 */
public class DatasourceUtility {

	/**
	 * テーブルのレコードをソートする。
	 * 
	 * @param aTable テーブル
	 * @param cols ソートフィールド名
	 */
	public static void sort(final Table aTable, final String... cols) {
		Collections.sort(aTable.getRecords(), new RecordComparator(aTable.getFields(), cols));
	}

	private static class RecordComparator implements Comparator<Record> {

		private List<String> cols;
		private Map<String, Field> fields;

		public RecordComparator(final List<Field> aFields, final String... aCols) {
			cols = new ArrayList<String>();
			fields = new HashMap<String, Field>();
			for (String col : aCols) {
				cols.add(col);
				for (Field field : aFields) {
					if (field.getName().equals(col)) {
						fields.put(col, field);
						break;
					}
				}
			}
		}

		@Override
		public int compare(final Record aRecord1, final Record aRecord2) {

			for (String col : cols) {
				Object obj1 = aRecord1.get(col);
				Object obj2 = aRecord2.get(col);

				if (obj1 == obj2) {

				} else if (null == obj1) {
					return 1;
				} else if (null == obj2) {
					return -1;
				} else {
					int rslt = obj1.toString().compareTo(obj2.toString());
					if (0 != rslt) {
						return rslt;
					}
				}
			}
			return 0;
		}

	}
}
