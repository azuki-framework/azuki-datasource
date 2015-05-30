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
package org.azkfw.datasource.excel;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.DatasourceBuilder;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.FieldType;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;
import org.azkfw.util.StringUtility;

/**
 * このクラスは、Excel(xlsx)ファイルからデータソースを構築するビルダークラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/07/31
 * @author Kawakicchi
 */
public final class ExcelDatasourceBuilder extends DatasourceBuilder {

	/**
	 * デフォルトのNULL文字列
	 */
	private static final String DEFAULT_NULL_STRING = "(NULL)";

	/**
	 * テーブル名称取得パターン
	 * <p>
	 * シート名からテーブル名とラベルを取得するためのパターン
	 * </p>
	 */
	private static final Pattern PTN_TABLE_NAME = Pattern.compile("^(.+?)(\\((.*?){1}\\).*?){0,1}$");

	/** データソース名 */
	private String datasourceName;
	/** NULL文字列 */
	private String nullString;
	/** Excelファイル一覧 */
	private List<File> excelFiles;

	/**
	 * コンストラクタ
	 */
	private ExcelDatasourceBuilder() {
		super(ExcelDatasourceBuilder.class);
		datasourceName = null;
		nullString = DEFAULT_NULL_STRING;
		excelFiles = new ArrayList<File>();
	}

	/**
	 * コンストラクタ
	 * 
	 * @param name データソース名
	 */
	private ExcelDatasourceBuilder(final String name) {
		super(ExcelDatasourceBuilder.class);
		datasourceName = name;
		nullString = DEFAULT_NULL_STRING;
		excelFiles = new ArrayList<File>();
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance() {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder();
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param file Excel(xlsx)ファイル
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final File file) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder();
		builder = builder.addFile(file);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param files Excel(xlsx)ファイル一覧
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final List<File> files) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder();
		builder = builder.addFiles(files);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final String name) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder(name);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @param file Excel(xlsx)ファイル
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final String name, final File file) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder(name);
		builder = builder.addFile(file);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @param files Excel(xlsx)ファイル一覧
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final String name, final List<File> files) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder(name);
		builder = builder.addFiles(files);
		return builder;
	}

	/**
	 * データソース名を設定する。
	 * 
	 * @param name データソース名
	 * @return ビルダー
	 */
	public ExcelDatasourceBuilder setDatasourceName(final String name) {
		datasourceName = name;
		return this;
	}

	/**
	 * Excel(xlsx)ファイルを追加する。
	 * 
	 * @param file Excel(xlsx)ファイル
	 * @return ビルダー
	 */
	public ExcelDatasourceBuilder addFile(final File file) {
		excelFiles.add(file);
		return this;
	}

	/**
	 * Excel(xlsx)ファイル一覧を追加する。
	 * 
	 * @param files Excel(xlsx)ファイル一覧
	 * @return ビルダー
	 */
	public ExcelDatasourceBuilder addFiles(final Collection<File> files) {
		excelFiles.addAll(files);
		return this;
	}

	/**
	 * NULL文字列を設定する。
	 * 
	 * @param string NULL文字列
	 * @return ビルダー
	 */
	public ExcelDatasourceBuilder setNullString(final String string) {
		nullString = string;
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
		ExcelDatasource datasource = new ExcelDatasource();
		datasource.name = datasourceName;

		InputStream stream = null;
		try {
			List<Table> tables = new ArrayList<>();

			for (File file : excelFiles) {
				stream = new FileInputStream(file);
				XSSFWorkbook workbook = new XSSFWorkbook(stream);

				int cntSheet = workbook.getNumberOfSheets();
				for (int i = 0; i < cntSheet; i++) {
					// sheet name -> table name
					String sheetName = workbook.getSheetName(i);
					ExcelTable table = new ExcelTable();

					Matcher matcher = PTN_TABLE_NAME.matcher(sheetName);
					if (matcher.find()) {
						table.label = matcher.group(3);
						table.name = matcher.group(1);
					} else {
						table.label = sheetName;
						table.name = sheetName;
					}

					XSSFSheet sheet = workbook.getSheetAt(i);
					// Check row size
					int cntRow = sheet.getLastRowNum() + 1;
					if (3 > cntRow) {
						warn(String.format("Skip sheet[%s]. row size < 3", sheetName));
						continue;
					}

					// Read Field
					List<ExcelField> fields = new ArrayList<ExcelField>();
					XSSFRow rowLabel = sheet.getRow(0);
					XSSFRow rowName = sheet.getRow(1);
					XSSFRow rowType = sheet.getRow(2);
					for (int col = 0; col < rowLabel.getLastCellNum(); col++) {
						ExcelField field = readField(col, rowLabel.getCell(col), rowName.getCell(col), rowType.getCell(col));
						fields.add(field);
					}

					// Read Data
					List<ExcelRecord> records = new ArrayList<ExcelRecord>();
					for (int row = 3; row < cntRow; row++) {
						XSSFRow xssfrow = sheet.getRow(row);
						if (!isEmptyRow(xssfrow)) {
							ExcelRecord record = readData(row, xssfrow, fields);
							records.add(record);
						} else {
							warn(String.format("Skip empty row.[table: %s; row: %d;]", table.getName(), row));
						}
					}

					table.fields = (List) fields;
					table.records = (List) records;

					tables.add(table);
				}
			}

			datasource.tables = tables;

		} catch (FileNotFoundException ex) {
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

	private ExcelField readField(final int col, final XSSFCell labelCell, final XSSFCell nameCell, final XSSFCell typeCell) throws ParseException {
		String label = toStringFromCell(labelCell);
		String name = toStringFromCell(nameCell);
		String type = toStringFromCell(typeCell);

		if (StringUtility.isEmpty(name)) {
			throw new ParseException(String.format("Field name is empty.[row: 1; col: %d;]", col), 1);
		}
		if (StringUtility.isEmpty(type)) {
			throw new ParseException(String.format("Field type is empty.[row: 2; col: %d;]", col), 2);
		}

		FieldType fieldType = FieldType.valueOfName(type.trim());
		if (FieldType.Unknown == fieldType) {
			throw new ParseException(String.format("Undefined type.[type: %s; row: 2; col: %d;]", type, col), 2);
		}

		ExcelField field = new ExcelField();
		field.label = label;
		field.name = name;
		field.type = fieldType;
		field.col = col;
		return field;
	}

	private ExcelRecord readData(final int rowNum, final XSSFRow row, final List<ExcelField> fields) throws ParseException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < fields.size(); i++) {
			ExcelField field = fields.get(i);

			int col = field.col;
			XSSFCell cell = row.getCell(col);

			String value = toStringFromCell(cell);

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
						if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
							obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
						} else {
							obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
						}
					}
				} else if (FieldType.Date == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Timestamp ts = null;
						if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
							ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd").parse(value).getTime());
						} else {
							ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
						}
						obj = new Date(ts.getTime());
					}
				} else if (FieldType.Time == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Timestamp ts = null;
						if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
							ts = new Timestamp(new SimpleDateFormat("HH:mm:ss").parse(value).getTime());
						} else {
							ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
						}
						obj = new Time(ts.getTime());
					}
				} else {
					throw new ParseException(String.format("Undefined type.[%s]", field.getType()), rowNum);
				}
			}

			data.put(field.name, obj);
		}

		ExcelRecord record = new ExcelRecord();
		record.data = data;
		return record;
	}

	private boolean isEmptyRow(final XSSFRow row) {
		for (int col = 0; col < row.getLastCellNum(); col++) {
			XSSFCell cell = row.getCell(col);
			String value = toStringFromCell(cell);
			if (0 < value.length()) {
				return false;
			}
		}
		return true;
	}

	private String toStringFromCell(final Cell cell) { // データ型毎の読み取り
		String string = "";

		if (null != cell) {
			switch (cell.getCellType()) {
			case Cell.CELL_TYPE_BLANK:
				break;
			case Cell.CELL_TYPE_BOOLEAN:
				string = Boolean.toString(cell.getBooleanCellValue());
				break;
			case Cell.CELL_TYPE_FORMULA:
				string = cell.getCellFormula();
				// string = cell.getStringCellValue();(※）
				break;
			case Cell.CELL_TYPE_NUMERIC:
				if (DateUtil.isCellDateFormatted(cell)) {
					java.util.Date dt = cell.getDateCellValue();
					string = (new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")).format(dt);
				} else {
					string = Double.toString(cell.getNumericCellValue());
				}
				break;
			case Cell.CELL_TYPE_STRING: {
				string = cell.getStringCellValue();
				break;
			}
			case Cell.CELL_TYPE_ERROR: {
				break;
			}
			}
		}
		return string;
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
		}
	}

	/**
	 * このクラスは、Excel(xlsx)用のデータソース情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class ExcelDatasource implements Datasource {

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
	 * このクラスは、Excel(xlsx)用のテーブル情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class ExcelTable implements Table {

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
	 * このクラスは、Excel(xlsx)ファイル用のフィールド情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class ExcelField implements Field {

		private String label;
		private String name;
		private FieldType type;

		private int col;

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
	 * このクラスは、Excel(xlsx)ファイル用のレコード情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class ExcelRecord implements Record {

		private Map<String, Object> data;

		@Override
		public Object get(final String aName) {
			return data.get(aName);
		}

	}
}
