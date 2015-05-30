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
package org.azkfw.datasource.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.digester3.Digester;
import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.DatasourceBuilder;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.FieldType;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;
import org.azkfw.util.StringUtility;
import org.xml.sax.SAXException;

/**
 * このクラスは、XMLファイルからデータソースを構築するビルダークラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/08/01
 * @author Kawakicchi
 */
public final class XmlDatasourceBuilder extends DatasourceBuilder {

	/**
	 * デフォルトのNULL文字列
	 */
	private static final String DEFAULT_NULL_STRING = "(NULL)";

	/** データソース名 */
	private String datasourceName;
	/** NULL文字列 */
	private String nullString;
	/** XMLファイル一覧 */
	private List<File> xmlFiles;

	/**
	 * コンストラクタ
	 */
	private XmlDatasourceBuilder() {
		super(XmlDatasourceBuilder.class);
		datasourceName = null;
		nullString = DEFAULT_NULL_STRING;
		xmlFiles = new ArrayList<File>();
	}

	/**
	 * コンストラクタ
	 * 
	 * @param name データソース名
	 */
	private XmlDatasourceBuilder(final String name) {
		super(XmlDatasourceBuilder.class);
		datasourceName = name;
		nullString = DEFAULT_NULL_STRING;
		xmlFiles = new ArrayList<File>();
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @return 新規ビルダー
	 */
	public static XmlDatasourceBuilder newInstance() {
		XmlDatasourceBuilder builder = new XmlDatasourceBuilder();
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param file XMLファイル
	 * @return 新規ビルダー
	 */
	public static XmlDatasourceBuilder newInstance(final File file) {
		XmlDatasourceBuilder builder = new XmlDatasourceBuilder();
		builder = builder.addFile(file);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param files XMLファイル一覧
	 * @return 新規ビルダー
	 */
	public static XmlDatasourceBuilder newInstance(final List<File> files) {
		XmlDatasourceBuilder builder = new XmlDatasourceBuilder();
		builder = builder.addFiles(files);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @return 新規ビルダー
	 */
	public static XmlDatasourceBuilder newInstance(final String name) {
		XmlDatasourceBuilder builder = new XmlDatasourceBuilder(name);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @param file XMLファイル
	 * @return 新規ビルダー
	 */
	public static XmlDatasourceBuilder newInstance(final String name, final File file) {
		XmlDatasourceBuilder builder = new XmlDatasourceBuilder(name);
		builder = builder.addFile(file);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @param files XMLファイル一覧
	 * @return 新規ビルダー
	 */
	public static XmlDatasourceBuilder newInstance(final String name, final List<File> files) {
		XmlDatasourceBuilder builder = new XmlDatasourceBuilder(name);
		builder = builder.addFiles(files);
		return builder;
	}

	/**
	 * データソース名を設定する。
	 * 
	 * @param name データソース名
	 * @return ビルダー
	 */
	public XmlDatasourceBuilder setDatasourceName(final String name) {
		datasourceName = name;
		return this;
	}

	/**
	 * XMLファイルを追加する。
	 * 
	 * @param file XMLファイル
	 * @return ビルダー
	 */
	public XmlDatasourceBuilder addFile(final File file) {
		xmlFiles.add(file);
		return this;
	}

	/**
	 * XMLファイル一覧を追加する。
	 * 
	 * @param files XMLファイル一覧
	 * @return ビルダー
	 */
	public XmlDatasourceBuilder addFiles(final Collection<File> files) {
		xmlFiles.addAll(files);
		return this;
	}

	/**
	 * NULL文字列を設定する。
	 * 
	 * @param string NULL文字列
	 * @return ビルダー
	 */
	public XmlDatasourceBuilder setNullString(final String string) {
		nullString = string;
		return this;
	}

	/**
	 * データソースを構築する。
	 * 
	 * @return データソース
	 * @throws FileNotFoundException
	 * @throws ParseException
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Datasource build() throws ParseException {
		XmlDatasource datasource = new XmlDatasource();
		datasource.name = datasourceName;

		InputStream stream = null;
		try {
			List<XmlTable> tables = new ArrayList<XmlTable>();

			for (File file : xmlFiles) {
				List<XmlTableEntity> tableList = null;

				stream = new FileInputStream(file);
				Digester digester = new Digester();

				digester.addObjectCreate("datasource/tables", ArrayList.class);

				digester.addObjectCreate("datasource/tables/table", XmlTableEntity.class);
				digester.addSetProperties("datasource/tables/table");
				digester.addSetNext("datasource/tables/table", "add");

				digester.addObjectCreate("datasource/tables/table/fields", ArrayList.class);
				digester.addSetNext("datasource/tables/table/fields", "setFields");

				digester.addObjectCreate("datasource/tables/table/fields/field", XmlFieldEntity.class);
				digester.addSetProperties("datasource/tables/table/fields/field");
				digester.addSetNext("datasource/tables/table/fields/field", "add");

				digester.addObjectCreate("datasource/tables/table/records", ArrayList.class);
				digester.addSetNext("datasource/tables/table/records", "setRecords");

				digester.addObjectCreate("datasource/tables/table/records/record", XmlRecordEntity.class);
				digester.addSetNext("datasource/tables/table/records/record", "add");

				digester.addObjectCreate("datasource/tables/table/records/record/data", XmlRecordDataEntity.class);
				digester.addSetProperties("datasource/tables/table/records/record/data");
				digester.addSetNext("datasource/tables/table/records/record/data", "add");

				tableList = digester.parse(stream);

				for (XmlTableEntity t : tableList) {
					XmlTable table = new XmlTable();
					table.label = t.label;
					table.name = t.name;

					// Read Field
					List<XmlField> fields = new ArrayList<XmlField>();
					for (int col = 0; col < t.fields.size(); col++) {
						XmlFieldEntity f = t.fields.get(col);
						XmlField field = readField(col, f);
						fields.add(field);
					}

					// Read Data
					List<XmlRecord> records = new ArrayList<XmlRecord>();
					for (int row = 0; row < t.records.size(); row++) {
						XmlRecordEntity r = t.records.get(row);
						if (r.data.size() == fields.size()) {
							XmlRecord record = readData(row, r, fields);
							records.add(record);
						} else {
							System.out.println("Skip row(unmatch field count).[table: " + table.getName() + "; row: " + r + ";]");
						}
					}

					table.fields = (List) fields;
					table.records = (List) records;

					tables.add(table);
				}
			}

			datasource.tables = (List) tables;

		} catch (SAXException ex) {
			fatal(ex);
			throw new ParseException(ex.getMessage(), -1);
		} catch (IOException ex) {
			fatal(ex);
			throw new ParseException(ex.getMessage(), -1);
		} finally {
			release(stream);
		}

		return datasource;
	}

	/**
	 * フィールド情報を読み込む。
	 * 
	 * @param col 列番号(0始まり)
	 * @param entity エンティティ情報
	 * @return フィールド情報
	 * @throws ParseException
	 */
	private XmlField readField(final int col, final XmlFieldEntity entity) throws ParseException {
		if (StringUtility.isEmpty(entity.name)) {
			throw new ParseException(String.format("Field name is empty.[col: %d;]", col), -1);
		}
		if (StringUtility.isEmpty(entity.type)) {
			throw new ParseException(String.format("Field type is empty.[col: %d;]", col), -1);
		}

		FieldType fieldType = FieldType.valueOfName(entity.type.trim());
		if (FieldType.Unknown == fieldType) {
			throw new ParseException(String.format("Undefined type.[type: %s; col: %d;]", entity.type, col), -1);
		}

		XmlField field = new XmlField();
		field.label = entity.label;
		field.name = entity.name;
		field.type = fieldType;
		return field;
	}

	private XmlRecord readData(final int rowNum, final XmlRecordEntity entity, final List<XmlField> fields) throws ParseException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < fields.size(); i++) {
			XmlField field = fields.get(i);
			XmlRecordDataEntity d = entity.data.get(i);

			String value = d.value;

			Object obj = null;
			if (nullString.equals(value)) {
			} else {
				if (FieldType.String == field.type) {
					obj = value;
				} else if (FieldType.Boolean == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						obj = Boolean.parseBoolean(value);
					}
				} else if (FieldType.Integer == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Double dbl = Double.parseDouble(value);
						obj = Integer.valueOf(dbl.intValue());
					}
				} else if (FieldType.Long == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Double dbl = Double.parseDouble(value);
						obj = Long.valueOf(dbl.longValue());
					}
				} else if (FieldType.Float == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						obj = Float.parseFloat(value);
					}
				} else if (FieldType.Double == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						obj = Double.parseDouble(value);
					}
				} else if (FieldType.Timestamp == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
					}
				} else if (FieldType.Date == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Timestamp ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd").parse(value).getTime());
						obj = new Date(ts.getTime());
					}
				} else if (FieldType.Time == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Timestamp ts = new Timestamp(new SimpleDateFormat("HH:mm:ss").parse(value).getTime());
						obj = new Time(ts.getTime());
					}
				} else {
					throw new ParseException(String.format("Undefined type.[%s]", field.getType()), rowNum);
				}
			}

			data.put(field.name, obj);
		}

		XmlRecord record = new XmlRecord();
		record.data = data;
		return record;
	}

	/**
	 * ストリームを解放する。
	 * 
	 * @param stream ストリーム
	 */
	private void release(final InputStream stream) {
		try {
			if (null != stream) {
				stream.close();
			}
		} catch (IOException ex) {
			warn("Strem close error.", ex);
		}
	}

	/**
	 * このクラスは、XML用のデータソース情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class XmlDatasource implements Datasource {

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
	 * このクラスは、XML用のテーブル情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class XmlTable implements Table {

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
	 * このクラスは、XMLファイル用のフィールド情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class XmlField implements Field {

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
	 * このクラスは、XMLファイル用のレコード情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class XmlRecord implements Record {

		private Map<String, Object> data;

		@Override
		public Object get(final String aName) {
			return data.get(aName);
		}

	}

	public static class XmlTableEntity {

		private String label;
		private String name;

		private List<XmlFieldEntity> fields;
		private List<XmlRecordEntity> records;

		public void setLabel(final String aLabel) {
			label = aLabel;
		}

		public void setName(final String aName) {
			name = aName;
		}

		public void setFields(final List<XmlFieldEntity> aFields) {
			fields = aFields;
		}

		public void setRecords(final List<XmlRecordEntity> aRecord) {
			records = aRecord;
		}
	}

	public static class XmlFieldEntity {
		private String label;
		private String name;
		private String type;

		public void setLabel(final String aLabel) {
			label = aLabel;
		}

		public void setName(final String aName) {
			name = aName;
		}

		public void setType(final String aType) {
			type = aType;
		}
	}

	public static class XmlRecordEntity {

		private List<XmlRecordDataEntity> data;

		public XmlRecordEntity() {
			data = new ArrayList<XmlRecordDataEntity>();
		}

		public void add(final XmlRecordDataEntity aData) {
			data.add(aData);
		}
	}

	public static class XmlRecordDataEntity {
		private String value;

		public void setValue(final String aValue) {
			value = aValue;
		}
	}
}
