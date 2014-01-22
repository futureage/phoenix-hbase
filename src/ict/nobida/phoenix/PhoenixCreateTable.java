package ict.nobida.phoenix;

import ict.nobida.utils.Init;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixCreateTable {

//	stmt.executeUpdate("create table IF NOT EXISTS "+init.tableName+" (k VARBINARY not null primary key," +
//	" \"b\".\"a\" BINARY(1), \"b\".\"d\" INTEGER, \"b\".\"dp\" BIGINT, \"b\".\"s\" BOOLEAN," +
//	" \"b\".\"sp\" DOUBLE,  \"d\".\"z\" VARCHAR) SALT_BUCKETS=16");
    String sql1 = "create table IF NOT EXISTS ";
    String sql2 = " (k BIGINT not null primary key," +
			" dp BIGINT, s BOOLEAN," +
			" sp DOUBLE,  z VARCHAR) SALT_BUCKETS=10";
    String sql3 = " (k BIGINT not null primary key, b.d INTEGER, b.dp BIGINT, b.s BOOLEAN, b.y DOUBLE, d.z VARCHAR) SALT_BUCKETS=";
    
    String sql4 = " (k BIGINT not null primary key," +
			"b.d VARCHAR, b.dp VARCHAR, b.s VARCHAR," +
			" b.sp VARCHAR,  d.z VARCHAR) SALT_BUCKETS=";
    
    public void createTable(String confname) throws SQLException, InterruptedException, ClassNotFoundException {
		init = new Init();
		init.init(confname);
		if(init.zkAddr==null) {
			System.out.println("please input zk address.");
			return;
		}
		Statement stmt = null;
		Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
		Connection con = null;
		con = DriverManager.getConnection("jdbc:phoenix:"+init.zkAddr);
		stmt = con.createStatement();
		
		stmt.executeUpdate("drop table if exists "+init.tableName );
		//ipdata = Utils.getIP4phoenix(init.preSplitsCount);
		//ipdata = Utils.getIP4phoenixmutable(init.dist, init.rand);

		sql3 += Init.preSplitsCount;
		PreparedStatement pp = con.prepareStatement(sql1+init.tableName+ sql3);
		pp.executeUpdate();
		con.close();
	}
	/**
	 * @param args
	 */
    Init init = null;
	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
		PhoenixCreateTable ct = new PhoenixCreateTable();
		if(args ==null || args.length ==0){
			System.out.println("please input the conf file");
			return ;
		}
		ct.createTable(args[0]);
	}

}
