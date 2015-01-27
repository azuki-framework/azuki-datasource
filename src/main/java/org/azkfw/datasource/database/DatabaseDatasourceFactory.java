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
package org.azkfw.datasource.database;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.FieldType;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;
import org.azkfw.util.StringUtility;

/**
 * このクラスは、データベースからデータソース生成を行うファクトリクラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/08/01
 * @author Kawakicchi
 */
public final class DatabaseDatasourceFactory {

	/**
	 * XMLファイルからデータソースを生成する。
	 * 
	 * @param aName データソース名
	 * @param aConnection コネクション情報
	 * @param aTables テーブル名群
	 * @return データソース
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Datasource generate(final String aName, final Connection aConnection, final String... aTables) throws FileNotFoundException,
			ParseException, IOException {
		DatabaseDatasource datasource = new DatabaseDatasource();
		datasource.name = aName;

		Statement st = null;
		ResultSet rs = null;
		try {
			List<DatabaseTable> tables = new ArrayList<DatabaseTable>();

			for (String tableName : aTables) {
				DatabaseTable table = new DatabaseTable();
				table.label = tableName;
				table.name = tableName;

				StringBuilder sql = new StringBuilder();
				sql.append("SELECT * FROM " + tableName);

				st = aConnection.createStatement();
				rs = st.executeQuery(sql.toString());

				// Read Field
				List<DatabaseField> fields = new ArrayList<DatabaseField>();
				ResultSetMetaData meta = rs.getMetaData();
				for (int col = 0; col < meta.getColumnCount(); col++) {
					DatabaseField field = readField(col, meta.getColumnLabel(col + 1), meta.getColumnLabel(col + 1), meta.getColumnType(col + 1));
					fields.add(field);
				}

				// Read Data
				List<DatabaseRecord> records = new ArrayList<DatabaseRecord>();
				int row = 0;
				while (rs.next()) {
					DatabaseRecord record = readData(row, rs, fields);
					records.add(record);

					row++;
				}

				table.fields = (List) fields;
				table.records = (List) records;

				tables.add(table);

				rs.close();
				rs = null;
				st.close();
				st = null;
			}

			datasource.tables = (List) tables;

		} catch (SQLException ex) {
			throw new ParseException(ex.getMessage(), -1);
		} finally {
			if (null != rs) {
				try {
					rs.close();
				} catch (SQLException ex) {

				} finally {
					rs = null;
				}
			}
			if (null != st) {
				try {
					st.close();
				} catch (SQLException ex) {

				} finally {
					st = null;
				}
			}
		}

		return datasource;
	}

	private static DatabaseField readField(final int aCol, final String aLabel, final String aName, final int aType) throws ParseException {
		if (StringUtility.isEmpty(aLabel)) {
			throw new ParseException("Field name is empty.[row: 2; col: " + aCol + ";]", -1);
		}

		FieldType fieldType = null;
		switch (aType) {
		case Types.CHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.VARCHAR:
			fieldType = FieldType.String;
			break;
		case Types.BOOLEAN:
		case Types.BIT:
			fieldType = FieldType.Boolean;
			break;
		case Types.INTEGER:
		case Types.TINYINT:
			fieldType = FieldType.Integer;
			break;
		case Types.BIGINT:
			fieldType = FieldType.Long;
			break;
		case Types.FLOAT:
			fieldType = FieldType.Float;
			break;
		case Types.DOUBLE:
			fieldType = FieldType.Double;
			break;
		case Types.TIMESTAMP:
			fieldType = FieldType.Timestamp;
			break;
		case Types.DATE:
			fieldType = FieldType.Date;
			break;
		case Types.TIME:
			fieldType = FieldType.Time;
			break;
		default:
			throw new ParseException("Undefined type.[name: " + aLabel + "; type: " + aType + "; row: 2; col: " + aCol + ";]", 2);
		}

		DatabaseField field = new DatabaseField();
		field.label = aLabel;
		field.name = aLabel;
		field.type = fieldType;

		return field;
	}

	private static DatabaseRecord readData(final int aRowNum, final ResultSet rs, final List<DatabaseField> aFields) throws ParseException,
			SQLException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < aFields.size(); i++) {
			DatabaseField field = aFields.get(i);

			if (FieldType.String == field.type) {
				data.put(field.name, rs.getString(field.name));
			} else if (FieldType.Boolean == field.type) {
				data.put(field.name, rs.getBoolean(field.name));
			} else if (FieldType.Integer == field.type) {
				BigDecimal decimal = rs.getBigDecimal(field.name);
				if (null == decimal) {
					data.put(field.name, null);
				} else {
					data.put(field.name, Integer.valueOf(decimal.intValue()));
				}
			} else if (FieldType.Long == field.type) {
				BigDecimal decimal = rs.getBigDecimal(field.name);
				if (null == decimal) {
					data.put(field.name, null);
				} else {
					data.put(field.name, Long.valueOf(decimal.longValue()));
				}
			} else if (FieldType.Float == field.type) {
				BigDecimal decimal = rs.getBigDecimal(field.name);
				if (null == decimal) {
					data.put(field.name, null);
				} else {
					data.put(field.name, Float.valueOf(decimal.floatValue()));
				}
			} else if (FieldType.Double == field.type) {
				BigDecimal decimal = rs.getBigDecimal(field.name);
				if (null == decimal) {
					data.put(field.name, null);
				} else {
					data.put(field.name, Double.valueOf(decimal.doubleValue()));
				}
			} else if (FieldType.Timestamp == field.type) {
				data.put(field.name, rs.getTimestamp(field.name));
			} else if (FieldType.Date == field.type) {
				data.put(field.name, rs.getDate(field.name));
			} else if (FieldType.Time == field.type) {
				data.put(field.name, rs.getTime(field.name));
			} else {
				throw new ParseException("Undefined type.[" + field.getType() + "]", aRowNum);
			}

		}

		DatabaseRecord record = new DatabaseRecord();
		record.data = data;
		return record;
	}

	private static final class DatabaseDatasource implements Datasource {

		private String name;
		private List<Table> tables;

		@Override
		public String getName() {
			return name;
		}

		@Override
		public List<Table> getTables() {
			return tables;
		}

	}

	private static final class DatabaseTable implements Table {

		private String label;
		private String name;
		private List<Field> fields;
		private List<Record> records;

		@Override
		public String getLabel() {
			return label;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public List<Field> getFields() {
			return fields;
		}

		@Override
		public List<Record> getRecords() {
			return records;
		}

	}

	private static final class DatabaseField implements Field {

		private String label;
		private String name;
		private FieldType type;

		@Override
		public String getLabel() {
			return label;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public FieldType getType() {
			return type;
		}

	}

	private static final class DatabaseRecord implements Record {

		private Map<String, Object> data;

		@Override
		public Object get(final String aName) {
			return data.get(aName);
		}

	}
}
