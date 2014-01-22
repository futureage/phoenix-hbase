package ict.nobida.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.antlr.grammar.v3.ANTLRParser.throwsSpec_return;

public class HBaseUtility {

	static {
		try {
			Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			//throw new HBaseException(e);
		}
	}

	public static  Connection getConnection() throws SQLException {
		String getDBConnectionString = "jdbc:phoenix:hadoop.10.60.1.121"; // 从配置文件中读取链接字符串
		try {
			Connection _Connection = DriverManager
					.getConnection(getDBConnectionString);
			return _Connection;
		} catch (SQLException e) {
			throw e;
			//throw new HBaseException(e.getMessage(), e);
		}
	}

}