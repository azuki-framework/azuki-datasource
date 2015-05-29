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
import java.util.Collection;
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
 * このクラスは、CSVファイルからデータソースを構築するビルダークラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2014/08/01
 * @author Kawakicchi
 */
public final class CsvDatasourceBuilder {

	/**
	 * デフォルトのNULL文字列
	 */
	private static final String DEFAULT_NULL_STRING = "(NULL)";

	/**
	 * テーブル名称取得パターン
	 * <p>
	 * ファイル名からテーブル名とラベルを取得するためのパターン
	 * </p>
	 */
	private static final Pattern PTN_FILE_NAME = Pattern.compile("^(.+?)(\\((.*?){1}\\).*?){0,1}\\..*?$");

	/** データソース名 */
	private String datasourceName;
	/** NULL文字列 */
	private String nullString;
	/** CSVファイル文字コード */
	private Charset charset;
	/** CSVファイル一覧 */
	private List<File> csvFiles;

	/**
	 * コンストラクタ
	 */
	private CsvDatasourceBuilder() {
		datasourceName = null;
		nullString = DEFAULT_NULL_STRING;
		charset = null;
		csvFiles = new ArrayList<File>();
	}

	/**
	 * コンストラクタ
	 * 
	 * @param aName データソース名
	 */
	private CsvDatasourceBuilder(final String aName) {
		datasourceName = aName;
		nullString = DEFAULT_NULL_STRING;
		charset = null;
		csvFiles = new ArrayList<File>();
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance() {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder();
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aFile CSVファイル
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final File aFile) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder();
		builder = builder.addFile(aFile);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aFiles CSVファイル一覧
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final List<File> aFiles) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder();
		builder = builder.addFiles(aFiles);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aName データソース名
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final String aName) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder(aName);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aName データソース名
	 * @param aFile CSVファイル
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final String aName, final File aFile) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder(aName);
		builder = builder.addFile(aFile);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param aName データソース名
	 * @param aFiles CSVファイル一覧
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final String aName, final List<File> aFiles) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder(aName);
		builder = builder.addFiles(aFiles);
		return builder;
	}

	/**
	 * データソース名を設定する。
	 * 
	 * @param aName データソース名
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder setDatasourceName(final String aName) {
		datasourceName = aName;
		return this;
	}

	/**
	 * CSVファイルを追加する。
	 * 
	 * @param aFile CSVファイル
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder addFile(final File aFile) {
		csvFiles.add(aFile);
		return this;
	}

	/**
	 * CSVファイル一覧を追加する。
	 * 
	 * @param aFiles CSVファイル一覧
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder addFiles(final Collection<File> aFiles) {
		csvFiles.addAll(aFiles);
		return this;
	}

	/**
	 * CSVファイルの文字コードを設定する。
	 * 
	 * @param aCharset 文字コード
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder setCharset(final Charset aCharset) {
		charset = aCharset;
		return this;
	}

	/**
	 * CSVファイルの文字コードを設定する。
	 * 
	 * @param aCharsetName 文字コード名
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder setCharset(final String aCharsetName) {
		charset = Charset.forName(aCharsetName);
		return this;
	}

	/**
	 * NULL文字列を設定する。
	 * 
	 * @param aString NULL文字列
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder setNullString(final String aString) {
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
		CsvDatasource datasource = new CsvDatasource();
		datasource.name = datasourceName;

		CsvBufferedReader reader = null;
		try {
			List<CsvTable> tables = new ArrayList<CsvTable>();

			for (File file : csvFiles) {
				CsvTable table = new CsvTable();

				Matcher matcher = PTN_FILE_NAME.matcher(file.getName());
				if (matcher.find()) {
					table.label = matcher.group(3);
					table.name = matcher.group(1);
				} else {
					table.label = file.getName();
					table.name = file.getName();
				}

				if (null == charset) {
					reader = new CsvBufferedReader(file);
				} else {
					reader = new CsvBufferedReader(file, charset);
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

	private CsvField readField(final int aCol, final String aLabel, final String aName, final String aType) throws ParseException {
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

	private CsvRecord readData(final int aRowNum, final List<String> aBuffer, final List<CsvField> aFields) throws ParseException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < aFields.size(); i++) {
			CsvField field = aFields.get(i);

			String value = aBuffer.get(i);

			if (null != nullString && nullString.equals(value)) {
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
						obj = new Timestamp(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(value).getTime());
					}
					data.put(field.name, obj);
				} else if (FieldType.Date == field.type) {
					Date obj = null;
					if (StringUtility.isNotEmpty(value)) {
						Timestamp ts = new Timestamp(new SimpleDateFormat("yyyy/MM/dd").parse(value).getTime());
						obj = new Date(ts.getTime());
					}
					data.put(field.name, obj);
				} else if (FieldType.Time == field.type) {
					Time obj = null;
					if (StringUtility.isNotEmpty(value)) {
						Timestamp ts = new Timestamp(new SimpleDateFormat("HH:mm:ss").parse(value).getTime());
						obj = new Time(ts.getTime());
					}
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

	/**
	 * このクラスは、CSV用のデータソース情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class CsvDatasource implements Datasource {

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
	 * このクラスは、CSV用のテーブル情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class CsvTable implements Table {

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
	 * このクラスは、CSVファイル用のフィールド情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class CsvField implements Field {

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
	 * このクラスは、CSVファイル用のレコード情報を保持するクラスです。
	 * 
	 * @since 1.0.0
	 * @version 1.0.0 2014/08/02
	 * @author Kawakicchi
	 */
	private final class CsvRecord implements Record {

		private Map<String, Object> data;

		@Override
		public Object get(final String aName) {
			return data.get(aName);
		}

	}
}
