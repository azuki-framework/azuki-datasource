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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
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
import org.azkfw.datasource.DatasourceBuilder;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.FieldType;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;
import org.azkfw.util.StringUtility;

/**
 * このクラスは、データベースからデータソースを構築するビルダークラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/08/01
 * @author Kawakicchi
 */
public final class DatabaseDatasourceBuilder extends DatasourceBuilder {

	/** データソース名 */
	private String datasourceName;
	/** テーブル名一覧 */
	private List<String> tableNames;

	/** データベースドライバ */
	private String databaseDriver;
	/** データーベースURL */
	private String databaseUrl;
	/** データベースユーザ名 */
	private String databaseUser;
	/** データベースパスワード */
	private String databasePassword;

	/**
	 * コンストラクタ
	 */
	private DatabaseDatasourceBuilder() {
		super(DatabaseDatasourceBuilder.class);
		datasourceName = null;
		tableNames = new ArrayList<String>();
	}

	/**
	 * コンストラクタ
	 * 
	 * @param aName データソース名
	 */
	private DatabaseDatasourceBuilder(final String aName) {
		super(DatabaseDatasourceBuilder.class);
		datasourceName = aName;
		tableNames = new ArrayList<String>();
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @return 新規ビルダー
	 */
	public static DatabaseDatasourceBuilder newInstance() {
		DatabaseDatasourceBuilder builder = new DatabaseDatasourceBuilder();
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aName データソース名
	 * @return 新規ビルダー
	 */
	public static DatabaseDatasourceBuilder newInstance(final String aName) {
		DatabaseDatasourceBuilder builder = new DatabaseDatasourceBuilder(aName);
		return builder;
	}

	/**
	 * データソース名を設定する。
	 * 
	 * @param name データソース名
	 * @return ビルダー
	 */
	public DatabaseDatasourceBuilder setDatasourceName(final String name) {
		datasourceName = name;
		return this;
	}

	/**
	 * データベース情報を設定する。
	 * 
	 * @param driver ドライバ
	 * @param url URL
	 * @param user ユーザ
	 * @param password パスワード
	 * @return ビルダー
	 */
	public DatabaseDatasourceBuilder setDatabase(final String driver, final String url, final String user, final String password) {
		databaseDriver = driver;
		databaseUrl = url;
		databaseUser = user;
		databasePassword = password;
		return this;
	}

	/**
	 * テーブル名を追加する。
	 * 
	 * @param name テーブル名
	 * @return ビルダー
	 */
	public DatabaseDatasourceBuilder addTable(final String name) {
		tableNames.add(name);
		return this;
	}

	/**
	 * テーブル名群を追加する。
	 * 
	 * @param name テーブル名群
	 * @return ビルダー
	 */
	public DatabaseDatasourceBuilder addTables(final String... names) {
		for (String name : names) {
			tableNames.add(name);
		}
		return this;
	}

	/**
	 * データソースを構築する。
	 * 
	 * @return データソース
	 * @throws ParseException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Datasource build() throws ParseException {
		DatabaseDatasource datasource = new DatabaseDatasource();
		datasource.name = datasourceName;

		Connection connection = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			List<DatabaseTable> tables = new ArrayList<DatabaseTable>();

			Class.forName(databaseDriver);
			connection = DriverManager.getConnection(databaseUrl, databaseUser, databasePassword);

			st = connection.createStatement();

			for (String tableName : tableNames) {
				DatabaseTable table = new DatabaseTable();
				table.label = tableName;
				table.name = tableName;

				StringBuilder sql = new StringBuilder();
				sql.append("SELECT * FROM " + tableName + ";");

				debug(String.format("Execute query.[SQL: %s]", sql.toString()));
				rs = st.executeQuery(sql.toString());

				// Read Field
				List<DatabaseField> fields = new ArrayList<DatabaseField>();
				ResultSetMetaData meta = rs.getMetaData();
				for (int col = 0; col < meta.getColumnCount(); col++) {
					DatabaseField field = readField(col, meta);
					fields.add(field);
				}

				// Read Data
				List<DatabaseRecord> records = new ArrayList<DatabaseRecord>();
				int row = 0;
				while (rs.next()) {
					DatabaseRecord record = readRecord(row, rs, fields);
					records.add(record);
					row++;
				}

				table.fields = (List) fields;
				table.records = (List) records;
				tables.add(table);

				info(String.format("Read table.[table: %s; record: %d;]", tableName, records.size()));

				rs.close();
				rs = null;
			}

			datasource.tables = (List) tables;

		} catch (ClassNotFoundException ex) {
			fatal(ex);
			throw new ParseException(ex.getMessage(), -1);
		} catch (SQLException ex) {
			fatal(ex);
			throw new ParseException(ex.getMessage(), -1);
		} finally {
			release(rs);
			release(st);
			release(connection);
		}
		return datasource;
	}

	/**
	 * フィールド情報を読み込む。
	 * 
	 * @param col 列番号(0始まり)
	 * @param label ラベル名
	 * @param name フィールド名
	 * @param type フィールドタイプ
	 * @return フィールド情報
	 * @throws ParseException
	 */
	private DatabaseField readField(final int colNum, final ResultSetMetaData meta) throws ParseException, SQLException {
		String name = meta.getColumnLabel(colNum + 1);
		String label = meta.getColumnLabel(colNum + 1);
		int type = meta.getColumnType(colNum + 1);
		String typeName = meta.getColumnTypeName(colNum + 1);

		if (StringUtility.isEmpty(name)) {
			throw new ParseException(String.format("Field name is empty.[col: %d;]", colNum), -1);
		}

		FieldType fieldType = null;
		switch (type) {
		case Types.CHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.VARCHAR:
			fieldType = FieldType.String;
			break;
		case Types.BOOLEAN:
			fieldType = FieldType.Boolean;
			break;
		case Types.INTEGER:
		case Types.TINYINT:
		case Types.BIT:
			fieldType = FieldType.Integer;
			break;
		case Types.BIGINT:
			fieldType = FieldType.Long;
			break;
		case Types.FLOAT:
			fieldType = FieldType.Float;
			break;
		case Types.NUMERIC: // TODO: NUMERIC対応
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
			throw new ParseException(String.format("Undefined type.[name: %s; type: %s(%d); col: %d;]", label, typeName, type, colNum), 2);
		}

		DatabaseField field = new DatabaseField();
		field.label = label;
		field.name = label;
		field.type = fieldType;
		return field;
	}

	/**
	 * レコード情報を読み込む。
	 * 
	 * @param rowNum 行番号(0始まり)
	 * @param rs {@link ResultSet}
	 * @param fields フィールド情報
	 * @return レコード情報
	 * @throws ParseException
	 * @throws SQLException
	 */
	private DatabaseRecord readRecord(final int rowNum, final ResultSet rs, final List<DatabaseField> fields) throws ParseException, SQLException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < fields.size(); i++) {
			DatabaseField field = fields.get(i);

			Object obj = null;
			if (FieldType.String == field.type) {
				obj = rs.getString(field.name);
			} else if (FieldType.Boolean == field.type) {
				obj = rs.getBoolean(field.name);
			} else if (FieldType.Integer == field.type) {
				BigDecimal decimal = rs.getBigDecimal(field.name);
				if (null != decimal) {
					obj = Integer.valueOf(decimal.intValue());
				}
			} else if (FieldType.Long == field.type) {
				BigDecimal decimal = rs.getBigDecimal(field.name);
				if (null != decimal) {
					obj = Long.valueOf(decimal.longValue());
				}
			} else if (FieldType.Float == field.type) {
				BigDecimal decimal = rs.getBigDecimal(field.name);
				if (null != decimal) {
					obj = Float.valueOf(decimal.floatValue());
				}
			} else if (FieldType.Double == field.type) {
				BigDecimal decimal = rs.getBigDecimal(field.name);
				if (null != decimal) {
					obj = Double.valueOf(decimal.doubleValue());
				}
			} else if (FieldType.Timestamp == field.type) {
				obj = rs.getTimestamp(field.name);
			} else if (FieldType.Date == field.type) {
				obj = rs.getDate(field.name);
			} else if (FieldType.Time == field.type) {
				obj = rs.getTime(field.name);
			} else {
				throw new ParseException(String.format("Undefined type.[%s]", field.getType()), rowNum);
			}

			data.put(field.name, obj);
		}

		DatabaseRecord record = new DatabaseRecord();
		record.data = data;
		return record;
	}

	/**
	 * ResultSetを解放する。
	 * 
	 * @param rs ResultSet
	 */
	private void release(final ResultSet rs) {
		try {
			if (null != rs) {
				if (!rs.isClosed()) {
					rs.close();
				}
			}
		} catch (SQLException ex) {
			warn("ResultSet release error.", ex);
		}
	}

	/**
	 * ステートメントを解放する。
	 * 
	 * @param s ステートメント
	 */
	private void release(final Statement s) {
		try {
			if (null != s) {
				if (!s.isClosed()) {
					s.close();
				}
			}
		} catch (SQLException ex) {
			warn("Statement release error.", ex);
		}
	}

	/**
	 * コネクションを解放する。
	 * 
	 * @param c コネクション
	 */
	private void release(final Connection c) {
		try {
			if (null != c) {
				if (!c.isClosed()) {
					c.close();
				}
			}
		} catch (SQLException ex) {
			warn("Connection release error.", ex);
		}
	}

	/**
	 * このクラスは、データベース用のデータソース情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class DatabaseDatasource implements Datasource {

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

	/**
	 * このクラスは、データベース用のテーブル情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class DatabaseTable implements Table {

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

	/**
	 * このクラスは、データベース用のフィールド情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class DatabaseField implements Field {

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

	/**
	 * このクラスは、データベース用のレコード情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class DatabaseRecord implements Record {

		private Map<String, Object> data;

		@Override
		public Object get(final String aName) {
			return data.get(aName);
		}

	}
}
