package ict.nobida.phoenix;
import ict.nobida.utils.Init;
import ict.nobida.utils.StatiThread;
import ict.nobida.utils.Utils;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Insert {
	
	PreparedStatement ppst1(PreparedStatement pp, long lrk) throws SQLException {
		//k BIGINT not null primary key,b.d VARCHAR, b.dp VARCHAR, b.s VARCHAR, b.sp VARCHAR,  d.z VARCHAR
		int now = init.rand.nextInt(Init.dist*Init.dist);
		pp.setLong(1, lrk);
		pp.setString(2, ipdata.get(now).get(0));
		pp.setString(3, ipdata.get(now).get(1));
		pp.setString(4, ipdata.get(now).get(2));
		pp.setString(5, ipdata.get(now).get(3));
		pp.setString(6, Utils.getRandomString(init.datasize+init.rand.nextInt(init.datasize)));
		return pp;
	}
	PreparedStatement ppst2(PreparedStatement pp, long lrk) throws SQLException {
		//(k BIGINT not null primary key, b.d INTEGER, b.dp BIGINT, b.s BOOLEAN, b.y DOUBLE, d.z VARCHAR)
		pp.setLong(1, lrk);
		pp.setInt(2, init.rand.nextInt(1000));
		pp.setLong(3, lrk);
		pp.setBoolean(4, init.rand.nextBoolean());
		pp.setDouble(5, init.rand.nextDouble());
		pp.setString(6, Utils.getRandomString(init.datasize));
		return pp;
	}
	volatile boolean first=true;
	class UpsertThread extends Thread {
		public long donenum = 0;
		String tableName = null;
		byte[] rowkey = new byte[8];
		long countPerThread;
		int prefix = 0;

		UpsertThread(String name, long count, int tprefix) {
			this.tableName = name;
			this.countPerThread = count;
			prefix = tprefix;
		}

		public void run() {
			try {
				Statement stmt = null;
//				try {
//					Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
//				} catch (ClassNotFoundException e) {
//					e.printStackTrace();
//				}
				long sds = System.currentTimeMillis();
				Connection con = DriverManager.getConnection("jdbc:phoenix:"+init.zkAddr);
				if(first) {
					long prestart = System.currentTimeMillis();
					System.out.println("prepare time is: "+(prestart - sds)+" ms");
					System.out.println("hahahah");
					first = false;
				}
				stmt = con.createStatement();
				PreparedStatement pp = con.prepareStatement("upsert into "+tableName+" values (?,?,?,?,?,?)");
				
				StringBuffer pte = new StringBuffer("upsert into "+tableName +" values (");
//				String dd = "upsert into ls11 values (1381368600988, 85745, false, 0.10926243556544424, 'aaaMU9')";
//				stmt.executeUpdate(dd);
				long st = Utils.timeChange(init.scanStart, null);
				for(long i=0;i<countPerThread;++i) {
					long ii = Init.counter.incrementAndGet();
					long lrk = st+ii;
//					//(k BIGINT not null primary key, d INTEGER, dp BIGINT, s BOOLEAN, y DOUBLE, z DOUBLE)
//					pte.append(String.valueOf(lrk)+", ");
//					pte.append(String.valueOf(init.rand.nextInt(100000))+", ");
//					pte.append(String.valueOf(String.valueOf(lrk))+", ");
//					pte.append(String.valueOf(init.rand.nextBoolean())+", ");
//					double d1 = init.rand.nextDouble();
//					pte.append(String.valueOf(init.rand.nextInt(100000))+", ");
//					//double d2 = init.rand.nextDouble();
//					pte.append("'"+Utils.getRandomString(init.datasize)+"') ");
//					//System.out.println("d1:"+d1 +" d2:"+ d2);
//					String te = pte.toString();
//					stmt.executeUpdate(te);
//					pte = new StringBuffer("upsert into "+tableName +" values (");
					
					pp = ppst2(pp,lrk);
					pp.executeUpdate();
					
					init.rtCount[prefix]++;
					if(i%init.cacheCount == 0) {
						con.commit();
					}
				}
				con.commit();
				pp.close();
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} 
		}
	}
	long insertstarttime=0;
	public void mutiput2(final String tableName, final int tt, long limit) throws InterruptedException{
		final UpsertThread putts[] = new UpsertThread[Init.threadN];
		
		long perlimit=-1;
		long perlimit0=-1;
		if(limit>0) {
			if(limit%Init.threadN > 0) {
				perlimit = limit/Init.threadN;
				perlimit0 = limit/Init.threadN+limit%Init.threadN;
			} else {
				perlimit = limit/Init.threadN;
				perlimit0=perlimit;
			}
		}
		for (int i = 0; i < putts.length; i++) {
			if(i==0)
				putts[i] = new UpsertThread(tableName,perlimit0, i);
			else
				putts[i] = new UpsertThread(tableName,perlimit, i);
			putts[i].setName("UpsertThread_" + i);
			putts[i].start();
		}
		long prestart = System.currentTimeMillis();
		System.out.println("prepare time is: "+(prestart - insertstarttime)+" ms");

		StatiThread th = new StatiThread(init.rtCount, init.tt);
		th.start();

		for (int i = 0; i < putts.length; i++) {
			putts[i].join();
		}
		th.stop();
		long insertendtime = System.currentTimeMillis();
		String ops = Utils.getresult(Init.insertCount,insertstarttime,insertendtime);
		System.out.println("insert "+Init.insertCount+", total time: "+(insertendtime - insertstarttime)
				+" ms, average through: "+ ops+"/s");
	}

    public List<List<String>> ipdata = new ArrayList<List<String>>();
    
	public void mutiupsert(String confname) throws SQLException, InterruptedException, ClassNotFoundException {
		insertstarttime = System.currentTimeMillis();
		init = new Init();
		init.init(confname);
		if(init.zkAddr==null) {
			System.out.println("please input zk address.");
			return;
		}
		Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
		mutiput2(init.tableName, init.tt, Init.insertCount);
	}
	Init init = null;
	public static void main(String[] args) throws SQLException, InterruptedException, ClassNotFoundException {
		Insert insert = new Insert();
		if(args==null || args.length ==0 ) {
			System.out.println("please input confName");
			return;
		}
		insert.mutiupsert(args[0]);
		return;
	}
	
	public void test() throws SQLException {
		Statement stmt = null;
		ResultSet rset = null;
		Connection con = DriverManager.getConnection("jdbc:phoenix:10.60.1.121");
		stmt = con.createStatement();
		byte[][] splits=null;
		splits=Utils.getSplitV2(3);
		//stmt.executeUpdate("CREATE TABLE TEST (HOST VARCHAR NOT NULL PRIMARY KEY, DESCRIPTION VARCHAR) SPLIT ON ('CS','EU','NA')");
		stmt.executeUpdate("drop table test");
		//stmt.executeUpdate("CREATE TABLE test (HOST VARBINARY NOT NULL PRIMARY KEY, DESCRIPTION VARCHAR)");// SPLIT ON ("+splits[0]+","+splits[1]+")");
		//stmt.executeUpdate("create table test (mykey integer not null primary key, a.mycolumn varchar)");
		// stmt.executeUpdate("create VIEW \"ls12\" (mykey BIGINT not null primary key, \"b\".\"a\" BINARY(1), \"b\".\"d\" INTEGER, \"b\".\"dp\" BIGINT, \"b\".\"s\" CHAR(1), \"b\".\"sp\" DOUBLE, \"b\".\"z\" BOOLEAN, \"d\".\"z\" VARCHAR)");

		byte[] a = {(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x01};
		PreparedStatement pp = con.prepareStatement("CREATE TABLE test (HOST VARBINARY NOT NULL PRIMARY KEY, DESCRIPTION VARCHAR) SPLIT ON (?,?)");
		pp.setBytes(1,splits[0]);
		pp.setBytes(2,splits[1]);
		pp.executeUpdate(); 

		pp = con.prepareStatement("upsert into TEST values (?,?)");
		pp.setBytes(1,a);
		pp.setString(2, "hahaitisok");
		pp.executeUpdate(); 
		//stmt.executeUpdate("upsert into TEST values ("+a+",'Helloddd')");
		//stmt.executeUpdate("upsert into test values (2,'World!')");
		con.commit();

		PreparedStatement statement = con.prepareStatement("select * from test");
		rset = statement.executeQuery();
		while (rset.next()) {
			System.out.println(Utils.toStringBinary(rset.getBytes("HOST"),0,rset.getBytes("HOST").length));
			System.out.println(rset.getString("DESCRIPTION"));
		}
		statement.close();
		con.close();
	}
}
