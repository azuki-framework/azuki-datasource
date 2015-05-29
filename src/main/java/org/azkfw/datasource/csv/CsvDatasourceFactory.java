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
package org.azkfw.datasource.csv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.FieldType;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;
import org.azkfw.io.CsvBufferedReader;
import org.azkfw.util.StringUtility;

/**
 * このクラスは、CSVファイルからデータソース生成を行うファクトリクラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/08/01
 * @author Kawakicchi
 * @deprecated {@link CsvDatasourceBuilder}
 */
public final class CsvDatasourceFactory {

	/**
	 * テーブル名称取得パターン
	 * <p>
	 * ファイル名からテーブル名とラベルを取得するためのパターン
	 * </p>
	 */
	private static final Pattern PTN_FILE_NAME = Pattern.compile("^(.+?)(\\((.*?){1}\\).*?){0,1}\\..*?$");

	/**
	 * Excelファイルからデータソースを生成する。
	 * 
	 * @param aName データソース名
	 * @param aFiles Csvファイル群
	 * @return データソース
	 */
	public static Datasource generate(final String aName, final File... aFiles) throws FileNotFoundException, ParseException, IOException {
		return generate(aName, null, aFiles);
	}

	/**
	 * Excelファイルからデータソースを生成する。
	 * 
	 * @param aName データソース名
	 * @param aCharset 文字コード
	 * @param aFiles Csvファイル群
	 * @return データソース
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Datasource generate(final String aName, final Charset aCharset, final File... aFiles) throws FileNotFoundException, ParseException,
			IOException {
		CsvDatasource datasource = new CsvDatasource();
		datasource.name = aName;

		CsvBufferedReader reader = null;
		try {
			List<CsvTable> tables = new ArrayList<CsvTable>();

			for (File file : aFiles) {
				CsvTable table = new CsvTable();

				Matcher matcher = PTN_FILE_NAME.matcher(file.getName());
				if (matcher.find()) {
					table.label = matcher.group(3);
					table.name = matcher.group(1);
				} else {
					table.label = file.getName();
					table.name = file.getName();
				}

				if (null == aCharset) {
					reader = new CsvBufferedReader(file);
				} else {
					reader = new CsvBufferedReader(file, aCharset);
				}

				// Read Field
				List<CsvField> fields = new ArrayList<CsvField>();
				List<String> rowLabel = reader.readCsvLine();
				List<String> rowName = reader.readCsvLine();
				List<String> rowType = reader.readCsvLine();
				for (int col = 0; col < rowLabel.size(); col++) {
					CsvField field = readField(col, rowLabel.get(col), rowName.get(col), rowType.get(col));
					fields.add(field);
				}

				// Read Data
				List<CsvRecord> records = new ArrayList<CsvRecord>();
				List<String> buffer = null;
				int row = 3;
				while (null != (buffer = reader.readCsvLine())) {
					if (buffer.size() == fields.size()) {
						CsvRecord record = readData(row, buffer, fields);
						records.add(record);
					} else {
						System.out.println("Skip row(unmatch field count).[table: " + table.getName() + "; row: " + row + ";]");
					}
					row++;
				}

				table.fields = (List) fields;
				table.records = (List) records;

				tables.add(table);

				reader.close();
				reader = null;
			}

			datasource.tables = (List) tables;

		} catch (FileNotFoundException ex) {
			throw ex;
		} finally {
			if (null != reader) {
				try {
					reader.close();
				} catch (IOException ex) {

				} finally {
					reader = null;
				}
			}
		}

		return datasource;
	}

	private static CsvField readField(final int aCol, final String aLabel, final String aName, final String aType) throws ParseException {
		if (StringUtility.isEmpty(aName)) {
			throw new ParseException("Field name is empty.[row: 2; col: " + aCol + ";]", 2);
		}
		if (StringUtility.isEmpty(aType)) {
			throw new ParseException("Field type is empty.[row: 2; col: " + aCol + ";]", 2);
		}

		FieldType fieldType = FieldType.valueOfName(aType.trim());
		if (FieldType.Unknown == fieldType) {
			throw new ParseException("Undefined type.[row: 2; col: " + aCol + ";]", 2);
		}

		CsvField field = new CsvField();
		field.label = aLabel;
		field.name = aName;
		field.type = fieldType;

		return field;
	}

	private static CsvRecord readData(final int aRowNum, final List<String> aBuffer, final List<CsvField> aFields) throws ParseException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < aFields.size(); i++) {
			CsvField field = aFields.get(i);

			String value = aBuffer.get(i);

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

		CsvRecord record = new CsvRecord();
		record.data = data;
		return record;
	}

	private static final class CsvDatasource implements Datasource {

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

	private static final class CsvTable implements Table {

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

	private static final class CsvField implements Field {

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

	private static final class CsvRecord implements Record {

		private Map<String, Object> data;

		@Override
		public Object get(final String aName) {
			return data.get(aName);
		}

	}
}
