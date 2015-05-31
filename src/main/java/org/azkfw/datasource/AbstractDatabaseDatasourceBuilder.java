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
package org.azkfw.datasource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Kawakicchi
 */
public class AbstractDatabaseDatasourceBuilder extends AbstractDatasourceBuilder {

	/** データベースドライバ */
	private String databaseDriver;
	/** データーベースURL */
	private String databaseUrl;
	/** データベースユーザ名 */
	private String databaseUser;
	/** データベースパスワード */
	private String databasePassword;

	/**
	 * コンストラクタ
	 * 
	 * @param clazz クラス
	 */
	protected AbstractDatabaseDatasourceBuilder(final Class<?> clazz) {
		super(clazz);
	}

	/**
	 * データベース情報を設定する。
	 * 
	 * @param driver ドライバ
	 * @param url URL
	 * @param user ユーザ
	 * @param password パスワード
	 * @return ビルダー
	 */
	public final void setDatabase(final String driver, final String url, final String user, final String password) {
		databaseDriver = driver;
		databaseUrl = url;
		databaseUser = user;
		databasePassword = password;
	}

	/**
	 * コネクションを作成する。
	 * 
	 * @return コネクション
	 * @throws ClassNotFoundException {@link ClassNotFoundException}
	 * @throws SQLException {@link SQLException}
	 */
	protected final Connection createConnection() throws ClassNotFoundException, SQLException {
		Class.forName(databaseDriver);
		Connection connection = DriverManager.getConnection(databaseUrl, databaseUser, databasePassword);
		return connection;
	}

	/**
	 * ResultSetを解放する。
	 * 
	 * @param rs ResultSet
	 */
	protected final void release(final ResultSet rs) {
		try {
			if (null != rs) {
				if (!rs.isClosed()) {
					rs.close();
				}
			}
		} catch (SQLException ex) {
			warn("ResultSet release error.", ex);
		}
	}

	/**
	 * ステートメントを解放する。
	 * 
	 * @param s ステートメント
	 */
	protected final void release(final Statement s) {
		try {
			if (null != s) {
				if (!s.isClosed()) {
					s.close();
				}
			}
		} catch (SQLException ex) {
			warn("Statement release error.", ex);
		}
	}

	/**
	 * コネクションを解放する。
	 * 
	 * @param c コネクション
	 */
	protected final void release(final Connection c) {
		try {
			if (null != c) {
				if (!c.isClosed()) {
					c.close();
				}
			}
		} catch (SQLException ex) {
			warn("Connection release error.", ex);
		}
	}
}
