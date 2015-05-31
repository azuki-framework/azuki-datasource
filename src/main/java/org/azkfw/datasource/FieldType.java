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

/**
 * この列挙クラスは、フィールドタイプを定義した列挙クラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/07/31
 * @author Kawakicchi
 */
public enum FieldType {

	/** Null */
	Null(0, "NULL"),

	/** 文字列 */
	String(1, "STRING"),
	/** 真偽 */
	Boolean(2, "BOOLEAN"),

	/** Integer */
	Integer(10, "INTEGER"),
	/** Long */
	Long(11, "LONG"),
	/** Float */
	Float(12, "FLOAT"),
	/** Double */
	Double(13, "DOUBLE"),
	/** Numeric */
	Numeric(14, "NUMERIC"),

	/** Timestamp */
	Timestamp(20, "TIMESTAMP"),
	/** Date */
	Date(21, "DATE"),
	/** Time */
	Time(22, "TIME"),

	/** Unknown */
	Unknown(-1, "UNKNOWN");

	/** タイプ */
	private int type;
	/** 名前 */
	private String name;

	private FieldType(final int aType, final String aName) {
		type = aType;
		name = aName;
	}

	/**
	 * タイプを取得する。
	 * 
	 * @return タイプ
	 */
	public int getType() {
		return type;
	}

	/**
	 * 名前を取得する。
	 * 
	 * @return 名前
	 */
	public String getName() {
		return name;
	}

	/**
	 * タイプから該当のフィールドタイプを取得する。
	 * 
	 * @param aType タイプ
	 * @return フィールドタイプ
	 */
	public static FieldType valueOfType(final int aType) {
		FieldType type = Unknown;
		for (FieldType ft : values()) {
			if (ft.type == aType) {
				type = ft;
				break;
			}
		}
		return type;
	}

	/**
	 * 名前から該当のフィールドタイプを取得する。
	 * 
	 * @param aName 名前
	 * @return フィールドタイプ
	 */
	public static FieldType valueOfName(final String aName) {
		FieldType type = Unknown;
		if (null != aName) {
			String name = aName.toUpperCase();
			for (FieldType ft : values()) {
				if (ft.name.equals(name)) {
					type = ft;
					break;
				}
			}
			if ("VARCHAR2".equals(name)) {
				return String;
			} else if("NUMBER".equals(name)) {
				return Numeric;
			}
		}
		return type;
	}

}
