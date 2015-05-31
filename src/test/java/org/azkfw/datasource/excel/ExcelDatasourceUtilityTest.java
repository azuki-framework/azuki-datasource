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

import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.database.DatabaseDatasourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;

/**
 * このクラスは、{@link ExcelDatasourceUtility}クラスの評価を行うテストクラスです。
 * 
 * @author Kawakicchi
 */
@RunWith(JUnit4.class)
public class ExcelDatasourceUtilityTest extends TestCase {

	@Test
	public void test() throws Exception {
		DatabaseDatasourceBuilder builder = DatabaseDatasourceBuilder.newInstance();
		builder.setDatabase("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/db_test", "tester", "tester");
		builder.addTables("td_profile", "td_schedule", "tm_account");
		Datasource ds = builder.build();
		
		File destFile = new File("./tmp/data.xlsx");
		
		ExcelDatasourceUtility.write(ds, destFile);
	}	
}
