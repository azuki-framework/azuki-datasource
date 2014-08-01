package org.azkfw.datasource.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.azkfw.datasource.Datasource;
import org.junit.Test;

public class DatabaseDatasourceFactoryTest extends TestCase {

	@Test
	public void testType() {

		Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/db_sample", "root", "root");

			Datasource datasource = DatabaseDatasourceFactory.generate("database", connection, "table");

			assertNotNull(datasource);

		} catch (Exception ex) {
			ex.printStackTrace();
			fail();
		} finally {
			if (null != connection) {
				try {
					connection.close();
				} catch (SQLException ex) {

				}
			}
		}

	}

}
