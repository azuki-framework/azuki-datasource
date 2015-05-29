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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.digester3.Digester;
import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.FieldType;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;
import org.azkfw.util.StringUtility;
import org.xml.sax.SAXException;

/**
 * このクラスは、XMLファイルからデータソース生成を行うファクトリクラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/08/01
 * @author Kawakicchi
 * @deprecated {@link XmlDatasourceBuilder}
 */
public final class XmlDatasourceFactory {

	/**
	 * XMLファイルからデータソースを生成する。
	 * 
	 * @param aFile XMLファイル
	 * @return データソース
	 */
	public static Datasource generate(final File aFile) throws FileNotFoundException, ParseException, IOException {
		return generate(aFile.getName(), aFile);
	}

	/**
	 * XMLファイルからデータソースを生成する。
	 * 
	 * @param aName データソース名
	 * @param aFile XMLファイル
	 * @return データソース
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Datasource generate(final String aName, final File aFile) throws FileNotFoundException, ParseException, IOException {
		XmlDatasource datasource = new XmlDatasource();
		datasource.name = aName;

		List<XmlTableEntity> tableList = null;

		InputStream stream = null;
		try {
			stream = new FileInputStream(aFile);
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
		} catch (SAXException ex) {
			throw new ParseException(ex.getMessage(), -1);
		} catch (IOException ex) {
			throw ex;
		} finally {
			if (null != stream) {
				try {
					stream.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

		List<XmlTable> tables = new ArrayList<XmlTable>();
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

		datasource.tables = (List) tables;
		return datasource;
	}

	private static XmlField readField(final int aCol, final XmlFieldEntity aField) throws ParseException {
		if (StringUtility.isEmpty(aField.name)) {
			throw new ParseException("Field name is empty.[row: 2; col: " + aCol + ";]", 2);
		}
		if (StringUtility.isEmpty(aField.type)) {
			throw new ParseException("Field type is empty.[row: 2; col: " + aCol + ";]", 2);
		}

		FieldType fieldType = FieldType.valueOfName(aField.type.trim());
		if (FieldType.Unknown == fieldType) {
			throw new ParseException("Undefined type.[type: " + aField.type + "; col: " + aCol + ";]", 2);
		}

		XmlField field = new XmlField();
		field.label = aField.label;
		field.name = aField.name;
		field.type = fieldType;

		return field;
	}

	private static XmlRecord readData(final int aRowNum, final XmlRecordEntity aRecord, final List<XmlField> aFields) throws ParseException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < aFields.size(); i++) {
			XmlField field = aFields.get(i);
			XmlRecordDataEntity d = aRecord.data.get(i);

			String value = d.value;

			if ("(NULL)".equals(value)) {
				data.put(field.name, null);
			} else {
				if (FieldType.String == field.type) {
					String obj = value;
					data.put(field.name, obj);
				} else if (FieldType.Boolean == field.type) {
					Boolean obj = Boolean.parseBoolean(value);
					data.put(field.name, obj);
				} else if (FieldType.Integer == field.type) {
					Double obj = Double.parseDouble(value);
					data.put(field.name, Integer.valueOf(obj.intValue()));
				} else if (FieldType.Long == field.type) {
					Double obj = Double.parseDouble(value);
					data.put(field.name, Long.valueOf(obj.longValue()));
				} else if (FieldType.Float == field.type) {
					Float obj = Float.parseFloat(value);
					data.put(field.name, obj);
				} else if (FieldType.Double == field.type) {
					Double obj = Double.parseDouble(value);
					data.put(field.name, obj);
				} else if (FieldType.Timestamp == field.type) {
					Timestamp obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
					data.put(field.name, obj);
				} else if (FieldType.Date == field.type) {
					Timestamp ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd").parse(value).getTime());
					Date obj = new Date(ts.getTime());
					data.put(field.name, obj);
				} else if (FieldType.Time == field.type) {
					Timestamp ts = new Timestamp(new SimpleDateFormat("HH:mm:ss").parse(value).getTime());
					Time obj = new Time(ts.getTime());
					data.put(field.name, obj);
				} else {
					throw new ParseException("Undefined type.[" + field.getType() + "]", aRowNum);
				}
			}
		}

		XmlRecord record = new XmlRecord();
		record.data = data;
		return record;
	}

	private static final class XmlDatasource implements Datasource {

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

	private static final class XmlTable implements Table {

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

	private static final class XmlField implements Field {

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

	private static final class XmlRecord implements Record {

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
