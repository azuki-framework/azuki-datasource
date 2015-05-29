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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;

/**
 * このクラスは、Excelデータソースを用のユーティリティクラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2015/05/27
 * @author Kawakicchi
 */
public final class ExcelDatasourceUtility {

	public static boolean write(final Datasource datasource, final File file) {
		
		XSSFWorkbook workbook = new XSSFWorkbook();
		write(datasource, workbook);
		OutputStream stream = null;
		try {
			stream = new BufferedOutputStream(new FileOutputStream(file));
			workbook.write(stream);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				stream.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return true;
	}
	
	private static void write(final Datasource datasource, final XSSFWorkbook workbook) {
		for (Table table : datasource.getTables()) {
			XSSFSheet sheet = workbook.createSheet(table.getName());
			write(table, sheet);
		}
	}
	
	private static void write(final Table table, final XSSFSheet sheet) {
		XSSFRow rowLabel = sheet.createRow(0);
		XSSFRow rowName = sheet.createRow(1);
		XSSFRow rowType = sheet.createRow(2);
		
		XSSFCell cell = null;
		List<Field> fields = table.getFields();
		for (int col = 0 ; col < fields.size() ; col++) {
			Field field = fields.get(col);
			
			cell = rowName.createCell(col);
			cell.setCellValue(field.getName());
		}
		
		XSSFRow rowRecord = null;
		List<Record> records = table.getRecords();
		for (int row = 0 ; row < records.size() ; row++ ) {
			rowRecord = sheet.createRow(row+3);
			Record record = records.get(row);
			for (int col = 0 ; col < fields.size() ; col++) {
				Field field = fields.get(col);
				Object value = record.get(field.getName());
				
				cell = rowRecord.createCell(col);
				if (null == value) {
					cell.setCellValue("(NULL)");
				} else {
					cell.setCellValue(value.toString());
				}
			}
		}
	}

}
