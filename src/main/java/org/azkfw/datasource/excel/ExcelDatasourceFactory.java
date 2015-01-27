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
 * このクラスは、Excel(xlsx)ファイルからデータソース生成を行うファクトリクラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/07/31
 * @author Kawakicchi
 */
public final class ExcelDatasourceFactory {

	/**
	 * テーブル名称取得パターン
	 * <p>
	 * シート名からテーブル名とラベルを取得するためのパターン
	 * </p>
	 */
	private static final Pattern PTN_TABLE_NAME = Pattern.compile("^(.+?)(\\((.*?){1}\\).*?){0,1}$");

	/**
	 * Excelファイルからデータソースを生成する。
	 * 
	 * @param aFile Excelファイル
	 * @return データソース
	 */
	public static Datasource generate(final File aFile) throws FileNotFoundException, ParseException, IOException {
		return generate(aFile.getName(), aFile);
	}

	/**
	 * Excelファイルからデータソースを生成する。
	 * 
	 * @param aName データソース名
	 * @param aFile Excelファイル
	 * @return データソース
	 */
	public static Datasource generate(final String aName, final File aFile) throws FileNotFoundException, ParseException, IOException {
		return generate(aName, new FileInputStream(aFile));
	}

	/**
	 * Excelファイルからデータソースを生成する。
	 * 
	 * @param aName データソース名
	 * @param aStream Excelストリーム
	 * @return データソース
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Datasource generate(final String aName, final InputStream aStream) throws FileNotFoundException, ParseException, IOException {
		ExcelDatasource datasource = new ExcelDatasource();
		datasource.name = aName;

		try {
			List<Table> tables = new ArrayList<>();

			XSSFWorkbook workbook = new XSSFWorkbook(aStream);
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

			datasource.tables = tables;

		} catch (FileNotFoundException ex) {
			throw ex;
		} catch (ParseException ex) {
			throw ex;
		} catch (IOException ex) {
			throw ex;
		} finally {
			if (null != aStream) {
				try {
					aStream.close();
				} catch (IOException ex) {
				}
			}
		}

		return datasource;
	}

	private static ExcelField readField(final int aCol, final XSSFCell aLabelCell, final XSSFCell aNameCell, final XSSFCell aTypeCell)
			throws ParseException {
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

	private static ExcelRecord readData(final int aRowNum, final XSSFRow aRow, final List<ExcelField> aFields) throws ParseException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < aFields.size(); i++) {
			ExcelField field = aFields.get(i);

			int col = field.col;
			XSSFCell cell = aRow.getCell(col);

			String value = toStringFromCell(cell);

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
					Timestamp obj = null;
					if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
						obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
					} else {
						obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
					}
					data.put(field.name, obj);
				} else if (FieldType.Date == field.type) {
					Timestamp ts = null;
					if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
						ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd").parse(value).getTime());
					} else {
						ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
					}
					Date obj = new Date(ts.getTime());
					data.put(field.name, obj);
				} else if (FieldType.Time == field.type) {
					Timestamp ts = null;
					if (Cell.CELL_TYPE_STRING == cell.getCellType()) {
						ts = new Timestamp(new SimpleDateFormat("HH:mm:ss").parse(value).getTime());
					} else {
						ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
					}
					Time obj = new Time(ts.getTime());
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

	private static boolean isEmptyRow(final XSSFRow aRow) {
		for (int col = 0; col < aRow.getLastCellNum(); col++) {
			XSSFCell cell = aRow.getCell(col);
			String value = toStringFromCell(cell);
			if (0 < value.length()) {
				return false;
			}
		}
		return true;
	}

	private static String toStringFromCell(final Cell aCell) { // データ型毎の読み取り
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

	private static final class ExcelDatasource implements Datasource {

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

	private static final class ExcelTable implements Table {

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

	private static final class ExcelField implements Field {

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

	private static final class ExcelRecord implements Record {

		private Map<String, Object> data;

		@Override
		public Object get(final String aName) {
			return data.get(aName);
		}

	}
}
