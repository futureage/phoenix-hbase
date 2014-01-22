package ict.nobida.phoenix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.antlr.grammar.v3.ANTLRParser.throwsSpec_return;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
public class HBaseHelper {

	static HBaseHelper _HBaseHelper = null;
	Connection _Connection = null;
	Statement _Statement = null;
	PreparedStatement _PreparedStatement = null;
	String _getExceptionInfoString = "";
	String _getDBConnectionString = "";

	private HBaseHelper() {}		

	/*
	 * Initialization
	 */
	public  HBaseHelper getInstanceBaseHelper() {
		if (_HBaseHelper == null)
			synchronized (HBaseHelper.class) {
				if(_HBaseHelper==null)
					_HBaseHelper = new HBaseHelper();
			}			
		return _HBaseHelper;
	}

	/*
	 * Insert , Delete , Update
	 */
	public Object ExcuteNonQuery(String sql) throws Exception {
		int n = 0;
		try {
			_Connection =HBaseUtility.getConnection();
			_Statement = _Connection.createStatement();
			n = _Statement.executeUpdate(sql);
			_Connection.commit();
		} catch (Exception e) {
			Dispose();
			throw e;
			//throw new HBaseException(e.getMessage(),e);
		} 
		return n;
	}

	public Object ExcuteNonQuery(String sql, Object[] args) throws SQLException {
		int n = 0;
		try {
			_Connection =HBaseUtility.getConnection();
			_PreparedStatement = _Connection.prepareStatement(sql);
			for (int i = 0; i < args.length; i++)
				_PreparedStatement.setObject(i + 1, args[i]);
			n = _PreparedStatement.executeUpdate();
			_Connection.commit();
		} catch (SQLException e) {
			Dispose();
			throw e;
			//throw new HBaseException(e.getMessage(),e);
		}
		return n;
	}

	/*
	 * Query
	 */
	public ResultSet ExecuteQuery(String sql) throws Exception {
		ResultSet rsResultSet = null;
		try {
			_Connection =HBaseUtility.getConnection();
			_Statement = _Connection.createStatement();
			rsResultSet = _Statement.executeQuery(sql);
		} catch (Exception e) {
			Dispose();
			throw e;
			//throw new HBaseException(e.getMessage(),e);
		} 
		return rsResultSet;
	}

	public ResultSet ExceteQuery(String sql, Object[] args) throws Exception {
		ResultSet rsResultSet = null;
		try {
			_Connection =HBaseUtility.getConnection();
			_PreparedStatement = _Connection.prepareStatement(sql);
			for (int i = 0; i < args.length; i++)
				_PreparedStatement.setObject(i + 1, args[i]);
			rsResultSet = _PreparedStatement.executeQuery();

		} catch (Exception e) {
			Dispose();
			throw e;
			//throw new HBaseException(e.getMessage(),e);
		} 
		return rsResultSet;
	}

	public void Dispose() {
		try {
			if (_Connection != null)
				_Connection.close();
			if (_Statement != null)
				_Statement.close();
		} catch (Exception e) {
			_getExceptionInfoString = e.getMessage();
		}
	}
}
