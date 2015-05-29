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
public final class ExcelDatasourceBuilder {

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
		datasourceName = null;
		nullString = DEFAULT_NULL_STRING;
		excelFiles = new ArrayList<File>();
	}

	/**
	 * コンストラクタ
	 * 
	 * @param aName データソース名
	 */
	private ExcelDatasourceBuilder(final String aName) {
		datasourceName = aName;
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
	 * @param aFile Excel(xlsx)ファイル
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final File aFile) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder();
		builder = builder.addFile(aFile);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aFiles Excel(xlsx)ファイル一覧
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final List<File> aFiles) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder();
		builder = builder.addFiles(aFiles);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aName データソース名
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final String aName) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder(aName);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aName データソース名
	 * @param aFile Excel(xlsx)ファイル
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final String aName, final File aFile) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder(aName);
		builder = builder.addFile(aFile);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aName データソース名
	 * @param aFiles Excel(xlsx)ファイル一覧
	 * @return 新規ビルダー
	 */
	public static ExcelDatasourceBuilder newInstance(final String aName, final List<File> aFiles) {
		ExcelDatasourceBuilder builder = new ExcelDatasourceBuilder(aName);
		builder = builder.addFiles(aFiles);
		return builder;
	}

	/**
	 * データソース名を設定する。
	 * 
	 * @param aName データソース名
	 * @return ビルダー
	 */
	public ExcelDatasourceBuilder setDatasourceName(final String aName) {
		datasourceName = aName;
		return this;
	}

	/**
	 * Excel(xlsx)ファイルを追加する。
	 * 
	 * @param aFile Excel(xlsx)ファイル
	 * @return ビルダー
	 */
	public ExcelDatasourceBuilder addFile(final File aFile) {
		excelFiles.add(aFile);
		return this;
	}

	/**
	 * Excel(xlsx)ファイル一覧を追加する。
	 * 
	 * @param aFiles Excel(xlsx)ファイル一覧
	 * @return ビルダー
	 */
	public ExcelDatasourceBuilder addFiles(final Collection<File> aFiles) {
		excelFiles.addAll(aFiles);
		return this;
	}

	/**
	 * NULL文字列を設定する。
	 * 
	 * @param aString NULL文字列
	 * @return ビルダー
	 */
	public ExcelDatasourceBuilder setNullString(final String aString) {
		nullString = aString;
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
	public Datasource build() throws FileNotFoundException, ParseException, IOException {
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
					String sheetName = workbook.getSheetName(i); // sheet name -> table name

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
						System.out.println("Skip sheet[" + sheetName + "]. row size < 3");
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
							System.out.println("Skip empty row.[table: " + table.getName() + "; row: " + row + ";]");
						}
					}

					table.fields = (List) fields;
					table.records = (List) records;

					tables.add(table);
				}
			}

			datasource.tables = tables;

		} catch (FileNotFoundException ex) {
			throw ex;
		} catch (ParseException ex) {
			throw ex;
		} catch (IOException ex) {
			throw ex;
		} finally {
			if (null != stream) {
				try {
					stream.close();
				} catch (IOException ex) {
				} finally {
					stream = null;
				}
			}
		}

		return datasource;
	}
	
	private ExcelField readField(final int aCol, final XSSFCell aLabelCell, final XSSFCell aNameCell, final XSSFCell aTypeCell) throws ParseException {
		String label = toStringFromCell(aLabelCell);
		String name = toStringFromCell(aNameCell);
		String type = toStringFromCell(aTypeCell);

		if (StringUtility.isEmpty(name)) {
			throw new ParseException("Field name is empty.[row: 2; col: " + aCol + ";]", 2);
		}
		if (StringUtility.isEmpty(type)) {
			throw new ParseException("Field type is empty.[row: 2; col: " + aCol + ";]", 2);
		}

		FieldType fieldType = FieldType.valueOfName(type.trim());
		if (FieldType.Unknown == fieldType) {
			throw new ParseException("Undefined type.[type: " + type + "; row: 2; col: " + aCol + ";]", 2);
		}

		ExcelField field = new ExcelField();
		field.label = label;
		field.name = name;
		field.type = fieldType;
		field.col = aCol;

		return field;
	}

	private ExcelRecord readData(final int aRowNum, final XSSFRow aRow, final List<ExcelField> aFields) throws ParseException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < aFields.size(); i++) {
			ExcelField field = aFields.get(i);

			int col = field.col;
			XSSFCell cell = aRow.getCell(col);

			String value = toStringFromCell(cell);

			if (nullString.equals(value)) {
				data.put(field.name, null);
			} else {
				if (FieldType.String == field.type) {
					String obj = value;
					data.put(field.name, obj);
				} else if (FieldType.Boolean == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Boolean obj = Boolean.parseBoolean(value);
						data.put(field.name, obj);
					} else {
						data.put(field.name, null);
					}
				} else if (FieldType.Integer == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Double obj = Double.parseDouble(value);
						data.put(field.name, Integer.valueOf(obj.intValue()));
					} else {
						data.put(field.name, null);
					}
				} else if (FieldType.Long == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Double obj = Double.parseDouble(value);
						data.put(field.name, Long.valueOf(obj.longValue()));
					} else {
						data.put(field.name, null);
					}
				} else if (FieldType.Float == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Float obj = Float.parseFloat(value);
						data.put(field.name, obj);
					} else {
						data.put(field.name, null);
					}
				} else if (FieldType.Double == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Double obj = Double.parseDouble(value);
						data.put(field.name, obj);
					} else {
						data.put(field.name, null);
					}
				} else if (FieldType.Timestamp == field.type) {
					Timestamp obj = null;
					if (StringUtility.isNotEmpty(value)) {
						if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
							obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
						} else {
							obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
						}
					}
					data.put(field.name, obj);
				} else if (FieldType.Date == field.type) {
					Date obj = null;
					if (StringUtility.isNotEmpty(value)) {
						Timestamp ts = null;
						if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
							ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd").parse(value).getTime());
						} else {
							ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
						}
						obj = new Date(ts.getTime());
					}
					data.put(field.name, obj);
				} else if (FieldType.Time == field.type) {
					Time obj = null;
					if (StringUtility.isNotEmpty(value)) {
						Timestamp ts = null;
						if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
							ts = new Timestamp(new SimpleDateFormat("HH:mm:ss").parse(value).getTime());
						} else {
							ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
						}
						obj = new Time(ts.getTime());
					}
					data.put(field.name, obj);
				} else {
					throw new ParseException("Undefined type.[" + field.getType() + "]", aRowNum);
				}
			}
		}

		ExcelRecord record = new ExcelRecord();
		record.data = data;
		return record;
	}

	private boolean isEmptyRow(final XSSFRow aRow) {
		for (int col = 0; col < aRow.getLastCellNum(); col++) {
			XSSFCell cell = aRow.getCell(col);
			String value = toStringFromCell(cell);
			if (0 < value.length()) {
				return false;
			}
		}
		return true;
	}

	private String toStringFromCell(final Cell aCell) { // データ型毎の読み取り
		String string = "";

		if (null != aCell) {
			switch (aCell.getCellType()) {
			case Cell.CELL_TYPE_BLANK:
				break;
			case Cell.CELL_TYPE_BOOLEAN:
				string = Boolean.toString(aCell.getBooleanCellValue());
				break;
			case Cell.CELL_TYPE_FORMULA:
				string = aCell.getCellFormula();
				// string = cell.getStringCellValue();(※）
				break;
			case Cell.CELL_TYPE_NUMERIC:
				if (DateUtil.isCellDateFormatted(aCell)) {
					java.util.Date dt = aCell.getDateCellValue();
					string = (new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")).format(dt);
				} else {
					string = Double.toString(aCell.getNumericCellValue());
				}
				break;
			case Cell.CELL_TYPE_STRING: {
				string = aCell.getStringCellValue();
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
