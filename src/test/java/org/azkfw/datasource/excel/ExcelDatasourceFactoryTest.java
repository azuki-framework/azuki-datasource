package org.azkfw.datasource.excel;

import java.io.File;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.FieldType;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;
import org.junit.Test;

public class ExcelDatasourceFactoryTest extends TestCase {

	@Test
	public void testType1() {
		File file = new File("./src/test/data/type.xlsx");

		try {
			Datasource datasource = ExcelDatasourceFactory.generate(file);

			assertNotNull(datasource);
			assertEquals("type.xlsx", datasource.getName());

			List<Table> tables = datasource.getTables();
			assertNotNull(tables);
			assertEquals("テーブル数", 1, tables.size());

			Table table = tables.get(0);
			assertNotNull(table);
			assertEquals("ラベル名", "テーブル1", table.getLabel());
			assertEquals("テーブル名", "Table1", table.getName());

			List<Field> fields = table.getFields();
			assertNotNull(fields);
			assertEquals("フィールド数", 9, fields.size());

			Field field = null;

			field = fields.get(0);
			assertNotNull(field);
			assertEquals("ラベル名", "文字列型", field.getLabel());
			assertEquals("フィールド名", "String", field.getName());
			assertEquals("フィールドタイプ", FieldType.String, field.getType());

			field = fields.get(1);
			assertNotNull(field);
			assertEquals("ラベル名", "Boolean型", field.getLabel());
			assertEquals("フィールド名", "Boolean", field.getName());
			assertEquals("フィールドタイプ", FieldType.Boolean, field.getType());

			field = fields.get(2);
			assertNotNull(field);
			assertEquals("ラベル名", "Integer型", field.getLabel());
			assertEquals("フィールド名", "Integer", field.getName());
			assertEquals("フィールドタイプ", FieldType.Integer, field.getType());

			field = fields.get(3);
			assertNotNull(field);
			assertEquals("ラベル名", "Long型", field.getLabel());
			assertEquals("フィールド名", "Long", field.getName());
			assertEquals("フィールドタイプ", FieldType.Long, field.getType());

			field = fields.get(4);
			assertNotNull(field);
			assertEquals("ラベル名", "Float型", field.getLabel());
			assertEquals("フィールド名", "Float", field.getName());
			assertEquals("フィールドタイプ", FieldType.Float, field.getType());

			field = fields.get(5);
			assertNotNull(field);
			assertEquals("ラベル名", "Double型", field.getLabel());
			assertEquals("フィールド名", "Double", field.getName());
			assertEquals("フィールドタイプ", FieldType.Double, field.getType());

			field = fields.get(6);
			assertNotNull(field);
			assertEquals("ラベル名", "Timestamp型", field.getLabel());
			assertEquals("フィールド名", "Timestamp", field.getName());
			assertEquals("フィールドタイプ", FieldType.Timestamp, field.getType());

			field = fields.get(7);
			assertNotNull(field);
			assertEquals("ラベル名", "Date型", field.getLabel());
			assertEquals("フィールド名", "Date", field.getName());
			assertEquals("フィールドタイプ", FieldType.Date, field.getType());

			field = fields.get(8);
			assertNotNull(field);
			assertEquals("ラベル名", "Time型", field.getLabel());
			assertEquals("フィールド名", "Time", field.getName());
			assertEquals("フィールドタイプ", FieldType.Time, field.getType());

			List<Record> records = table.getRecords();
			assertNotNull(records);
			assertEquals("レコード数", 2, records.size());

			Record record = null;

			record = records.get(0);
			assertNotNull(record);
			assertEquals("デフォルト", record.get("String"));
			assertEquals(Boolean.valueOf(true), record.get("Boolean"));
			assertEquals(Integer.valueOf(1), record.get("Integer"));
			assertEquals(Long.valueOf(1), record.get("Long"));
			assertEquals(Float.valueOf(1), record.get("Float"));
			assertEquals(Double.valueOf(1), record.get("Double"));
			assertEquals(Timestamp.valueOf("2014-12-31 23:59:59"), record.get("Timestamp"));
			assertEquals(toString(toDate(2014, 12, 31)), toString((Date) record.get("Date")));
			assertEquals(toString(Time.valueOf("23:59:59")), toString((Time) record.get("Time")));

			record = records.get(1);
			assertNotNull(record);
			assertEquals("デフォルト", record.get("String"));
			assertEquals(Boolean.valueOf(false), record.get("Boolean"));
			assertEquals(Integer.valueOf(2), record.get("Integer"));
			assertEquals(Long.valueOf(2), record.get("Long"));
			assertEquals(Float.valueOf(2), record.get("Float"));
			assertEquals(Double.valueOf(2), record.get("Double"));
			assertEquals(Timestamp.valueOf("2015-1-1 1:2:3"), record.get("Timestamp"));
			assertEquals(toString(toDate(2015, 1, 1)), toString((Date) record.get("Date")));
			assertEquals(toString(Time.valueOf("1:2:3")), toString((Time) record.get("Time")));

		} catch (Exception ex) {
			ex.printStackTrace();
			fail();
		}

	}

	@Test
	public void testType2() {
		File file = new File("./src/test/data/type.xlsx");

		try {
			Datasource datasource = ExcelDatasourceBuilder.newInstance("type.xlsx").addFile(file).build();

			assertNotNull(datasource);
			assertEquals("type.xlsx", datasource.getName());

			List<Table> tables = datasource.getTables();
			assertNotNull(tables);
			assertEquals("テーブル数", 1, tables.size());

			Table table = tables.get(0);
			assertNotNull(table);
			assertEquals("ラベル名", "テーブル1", table.getLabel());
			assertEquals("テーブル名", "Table1", table.getName());

			List<Field> fields = table.getFields();
			assertNotNull(fields);
			assertEquals("フィールド数", 9, fields.size());

			Field field = null;

			field = fields.get(0);
			assertNotNull(field);
			assertEquals("ラベル名", "文字列型", field.getLabel());
			assertEquals("フィールド名", "String", field.getName());
			assertEquals("フィールドタイプ", FieldType.String, field.getType());

			field = fields.get(1);
			assertNotNull(field);
			assertEquals("ラベル名", "Boolean型", field.getLabel());
			assertEquals("フィールド名", "Boolean", field.getName());
			assertEquals("フィールドタイプ", FieldType.Boolean, field.getType());

			field = fields.get(2);
			assertNotNull(field);
			assertEquals("ラベル名", "Integer型", field.getLabel());
			assertEquals("フィールド名", "Integer", field.getName());
			assertEquals("フィールドタイプ", FieldType.Integer, field.getType());

			field = fields.get(3);
			assertNotNull(field);
			assertEquals("ラベル名", "Long型", field.getLabel());
			assertEquals("フィールド名", "Long", field.getName());
			assertEquals("フィールドタイプ", FieldType.Long, field.getType());

			field = fields.get(4);
			assertNotNull(field);
			assertEquals("ラベル名", "Float型", field.getLabel());
			assertEquals("フィールド名", "Float", field.getName());
			assertEquals("フィールドタイプ", FieldType.Float, field.getType());

			field = fields.get(5);
			assertNotNull(field);
			assertEquals("ラベル名", "Double型", field.getLabel());
			assertEquals("フィールド名", "Double", field.getName());
			assertEquals("フィールドタイプ", FieldType.Double, field.getType());

			field = fields.get(6);
			assertNotNull(field);
			assertEquals("ラベル名", "Timestamp型", field.getLabel());
			assertEquals("フィールド名", "Timestamp", field.getName());
			assertEquals("フィールドタイプ", FieldType.Timestamp, field.getType());

			field = fields.get(7);
			assertNotNull(field);
			assertEquals("ラベル名", "Date型", field.getLabel());
			assertEquals("フィールド名", "Date", field.getName());
			assertEquals("フィールドタイプ", FieldType.Date, field.getType());

			field = fields.get(8);
			assertNotNull(field);
			assertEquals("ラベル名", "Time型", field.getLabel());
			assertEquals("フィールド名", "Time", field.getName());
			assertEquals("フィールドタイプ", FieldType.Time, field.getType());

			List<Record> records = table.getRecords();
			assertNotNull(records);
			assertEquals("レコード数", 2, records.size());

			Record record = null;

			record = records.get(0);
			assertNotNull(record);
			assertEquals("デフォルト", record.get("String"));
			assertEquals(Boolean.valueOf(true), record.get("Boolean"));
			assertEquals(Integer.valueOf(1), record.get("Integer"));
			assertEquals(Long.valueOf(1), record.get("Long"));
			assertEquals(Float.valueOf(1), record.get("Float"));
			assertEquals(Double.valueOf(1), record.get("Double"));
			assertEquals(Timestamp.valueOf("2014-12-31 23:59:59"), record.get("Timestamp"));
			assertEquals(toString(toDate(2014, 12, 31)), toString((Date) record.get("Date")));
			assertEquals(toString(Time.valueOf("23:59:59")), toString((Time) record.get("Time")));

			record = records.get(1);
			assertNotNull(record);
			assertEquals("デフォルト", record.get("String"));
			assertEquals(Boolean.valueOf(false), record.get("Boolean"));
			assertEquals(Integer.valueOf(2), record.get("Integer"));
			assertEquals(Long.valueOf(2), record.get("Long"));
			assertEquals(Float.valueOf(2), record.get("Float"));
			assertEquals(Double.valueOf(2), record.get("Double"));
			assertEquals(Timestamp.valueOf("2015-1-1 1:2:3"), record.get("Timestamp"));
			assertEquals(toString(toDate(2015, 1, 1)), toString((Date) record.get("Date")));
			assertEquals(toString(Time.valueOf("1:2:3")), toString((Time) record.get("Time")));

		} catch (Exception ex) {
			ex.printStackTrace();
			fail();
		}

	}

	private Date toDate(final int aYear, final int aMonth, final int aDay) {
		Calendar cln = Calendar.getInstance();
		cln.set(aYear, aMonth - 1, aDay, 0, 0, 0);
		return cln.getTime();
	}

	private String toString(final Date aDate) {
		String result = "";
		if (null != aDate) {
			if (aDate instanceof Date) {
				result = (new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")).format((Date) aDate);
			} else {
				result = aDate.getClass().getName();
			}
		}
		return result;
	}

	private String toString(final Time aTime) {
		String result = "";
		if (null != aTime) {
			if (aTime instanceof Time) {
				result = (new SimpleDateFormat("HH:mm:ss")).format(new Date(((Time) aTime).getTime()));
			} else {
				result = aTime.getClass().getName();
			}
		}
		return result;
	}
}
