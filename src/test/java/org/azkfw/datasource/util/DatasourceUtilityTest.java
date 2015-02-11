package org.azkfw.datasource.util;

import java.io.File;

import junit.framework.TestCase;

import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.Table;
import org.azkfw.datasource.excel.ExcelDatasourceFactory;
import org.junit.Test;

public class DatasourceUtilityTest extends TestCase {

	@Test
	public void testSort() {

		File file = new File("./src/test/data/sort.xlsx");

		try {
			Datasource datasource = ExcelDatasourceFactory.generate(file);

			Table table = datasource.getTables().get(0);

			DatasourceUtility.sort(table, "Time");

		} catch (Exception ex) {
			ex.printStackTrace();
			fail();
		}
	}

}
