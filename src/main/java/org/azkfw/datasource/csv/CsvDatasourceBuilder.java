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

import org.azkfw.datasource.AbstractTextDatasourceBuilder;
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
public final class CsvDatasourceBuilder extends AbstractTextDatasourceBuilder {

	/**
	 * テーブル名称取得パターン
	 * <p>
	 * ファイル名からテーブル名とラベルを取得するためのパターン
	 * </p>
	 */
	private static final Pattern PTN_FILE_NAME = Pattern.compile("^(.+?)(\\((.*?){1}\\).*?){0,1}\\..*?$");

	/** CSVファイル文字コード */
	private Charset charset;
	/** CSVファイル一覧 */
	private List<File> csvFiles;

	/**
	 * コンストラクタ
	 */
	private CsvDatasourceBuilder() {
		super(CsvDatasourceBuilder.class);
		charset = null;
		csvFiles = new ArrayList<File>();
	}

	/**
	 * コンストラクタ
	 * 
	 * @param name データソース名
	 */
	private CsvDatasourceBuilder(final String name) {
		super(CsvDatasourceBuilder.class);
		setName(name);
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
	 * @param file CSVファイル
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final File file) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder();
		builder = builder.addFile(file);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param files CSVファイル一覧
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final List<File> files) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder();
		builder = builder.addFiles(files);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final String name) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder(name);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @param file CSVファイル
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final String name, final File file) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder(name);
		builder = builder.addFile(file);
		return builder;
	}

	/**
	 * ビルダーを新規作成する。
	 * 
	 * @param name データソース名
	 * @param files CSVファイル一覧
	 * @return 新規ビルダー
	 */
	public static CsvDatasourceBuilder newInstance(final String name, final List<File> files) {
		CsvDatasourceBuilder builder = new CsvDatasourceBuilder(name);
		builder = builder.addFiles(files);
		return builder;
	}

	/**
	 * CSVファイルを追加する。
	 * 
	 * @param file CSVファイル
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder addFile(final File file) {
		csvFiles.add(file);
		return this;
	}

	/**
	 * CSVファイル一覧を追加する。
	 * 
	 * @param files CSVファイル一覧
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder addFiles(final Collection<File> files) {
		csvFiles.addAll(files);
		return this;
	}

	/**
	 * CSVファイルの文字コードを設定する。
	 * 
	 * @param charset 文字コード
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder setCharset(final Charset charset) {
		this.charset = charset;
		return this;
	}

	/**
	 * CSVファイルの文字コードを設定する。
	 * 
	 * @param charsetName 文字コード名
	 * @return ビルダー
	 */
	public CsvDatasourceBuilder setCharset(final String charsetName) {
		charset = Charset.forName(charsetName);
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
		CsvDatasource datasource = new CsvDatasource();
		datasource.name = getName();

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
						warn(String.format("Skip row(unmatch field count).[table: %s; row: %d;]", table.getName(), row));
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
			fatal(ex);
			throw new ParseException(ex.getMessage(), -1);
		} catch (IOException ex) {
			fatal(ex);
			throw new ParseException(ex.getMessage(), -1);
		} finally {
			release(reader);
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
	private CsvField readField(final int col, final String label, final String name, final String type) throws ParseException {
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

		CsvField field = new CsvField();
		field.label = label;
		field.name = name;
		field.type = fieldType;
		return field;
	}

	/**
	 * レコード情報を読み込む。
	 * 
	 * @param rowNum 行番号(0始まり)
	 * @param buffer データ
	 * @param fields フィールド情報
	 * @return レコード情報
	 * @throws ParseException
	 */
	private CsvRecord readData(final int rowNum, final List<String> buffer, final List<CsvField> fields) throws ParseException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i = 0; i < fields.size(); i++) {
			CsvField field = fields.get(i);

			String value = buffer.get(i);

			Object obj = null;
			if (!isNullString(value)) {
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
						Float flt = Float.parseFloat(value);
						obj = flt;
					}
				} else if (FieldType.Double == field.type) {
					if (StringUtility.isNotEmpty(value)) {
						Double dbl = Double.parseDouble(value);
						obj = dbl;
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
