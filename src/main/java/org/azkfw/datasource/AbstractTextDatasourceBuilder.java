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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * @author Kawakicchi
 */
public class AbstractTextDatasourceBuilder extends AbstractDatasourceBuilder {

	/**
	 * デフォルトのNULL文字列
	 */
	private static final String DEFAULT_NULL_STRING = "(NULL)";

	/** NULL文字列 */
	private String nullString;

	/**
	 * コンストラクタ
	 * 
	 * @param clazz クラス
	 */
	protected AbstractTextDatasourceBuilder(final Class<?> clazz) {
		super(clazz);
		nullString = DEFAULT_NULL_STRING;
	}

	/**
	 * NULL文字列を設定する。
	 * 
	 * @param string NULL文字列
	 */
	public final void setNullString(final String string) {
		nullString = string;
	}

	/**
	 * 文字列が<code>null</code>またはnull文字か判断する。
	 * 
	 * @param string 文字列
	 * @return <code>null</code>かnull文字列の場合、<code>true</code>を返す。
	 */
	protected final boolean isNullString(final String string) {
		if (null == string || (null != nullString && nullString.equals(string))) {
			return true;
		}
		return false;
	}

	/**
	 * ストリームを解放する。
	 * 
	 * @param stream ストリーム
	 */
	protected final void release(final InputStream stream) {
		try {
			if (null != stream) {
				stream.close();
			}
		} catch (IOException ex) {
			warn("Strem close error.", ex);
		}
	}

	/**
	 * リーダーを解放する。
	 * 
	 * @param reader リーダー
	 */
	protected final void release(final Reader reader) {
		try {
			if (null != reader) {
				reader.close();
			}
		} catch (IOException ex) {
			warn("Reader close error.", ex);
		}
	}
}
