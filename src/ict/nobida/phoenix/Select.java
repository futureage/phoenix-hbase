package ict.nobida.phoenix;
import ict.nobida.utils.Init;
import ict.nobida.utils.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class Select {
	public void mutiput2(final String tableName,final int tt, long limit) throws InterruptedException{
		long insertstarttime = System.nanoTime();

		long insertendtime = System.nanoTime();
		String ops = Utils.getresult(Init.insertCount,insertstarttime/1000/1000,insertendtime/1000/1000);
		System.out.println("insert "+Init.insertCount+", total time: "+(insertendtime - insertstarttime)/1000/1000
				+" ms, average through: "+ ops+"/s");
	}


	public void mutiget(String sql1) throws SQLException, InterruptedException {
		test(sql1);
	}
	Init init = null;
	long scanStartNum;
	long scanEndNum;
	String filterStr;
	String scanResultPath=null;
	long scanLimitCount;
	
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		Select select = new Select();
//		if(args == null || args.length ==0) {
//			System.out.println("please input the conf file");
//			return;
//		}
		String d = null;
		String dp= null;
		String s= null;
		String sp= null;
		String thenull = "null";
		select.init = new Init();
		select.init.init("putconf");
		for(int i=0;i< args.length; ++i) {
		String[] tmparg = args[i].split("=");
		if(tmparg[0].equals("st")) {
			if(!tmparg[1].equals(thenull))
				select.scanStartNum = Long.parseLong(tmparg[1]);
		} else if(tmparg[0].equals("et")) {
			if(!tmparg[1].equals(thenull))
				select.scanEndNum = Long.parseLong(tmparg[1]);
		} else if(tmparg[0].equals("result")) {
			if(!tmparg[1].equals(thenull))
				select.scanResultPath = tmparg[1];
		} else if(tmparg[0].equals("count")) {
			if(!tmparg[1].equals(thenull)){
				select.scanLimitCount = Long.parseLong(tmparg[1]);
			}
		} else if(tmparg[0].equals("s")) {
			if(!tmparg[1].equals(thenull)) {
				s = tmparg[1];
			}
		} else if(tmparg[0].equals("d")) {
			if(!tmparg[1].equals(thenull)){
				d = tmparg[1];
			}
		} else if(tmparg[0].equals("sp")) {
			if(!tmparg[1].equals(thenull)) {
				sp = tmparg[1];
			}
		} else if(tmparg[0].equals("dp")) {
			if(!tmparg[1].equals(thenull)){
				dp = tmparg[1];
			}
		}
		}
		long start = 0;
		long end =0;
		if(select.init.scanStart != null) {
			start = Utils.timeChange(select.init.scanStart, null) + select.scanStartNum;
			if(select.scanEndNum != 0)
				end = Utils.timeChange(select.init.scanStart, null) + select.scanEndNum;
		}
		if(start==-1 || end ==-1)
			return;
		String sql1 = " where ";
		String sql2 = null;
		boolean whe = false;
		if(start>0) {
			sql1 += " k>"+start;
			whe = true;
		}
		if(end>0){
			sql1 += " k<"+end;
			whe = true;
		}
		if(d!=null){
			sql1 += " d= "+d;
			whe = true;
		}
		if(dp!=null){
			sql1 += " dp= "+dp;
			whe = true;
		}
		if(s!=null) {
			sql1 += " s= "+s;
			whe = true;
		}
		if(sp!=null) {
			sql1 += " sp= "+sp;
			whe = true;
		}
		
		if(select.scanLimitCount !=0)
			sql2 = " limit "+select.scanLimitCount;
		String sql3 ="";
		if(whe)
			sql3 = sql1;
		if(sql2 != null)
			sql3 += sql2;
		System.out.println(sql3);
		select.mutiget(sql3);
		return;
	}
	long donenum=0;

	public void test(String sql1) throws SQLException {
		long insertstarttime = System.currentTimeMillis();
		ResultSet rset = null;
		Connection con = DriverManager.getConnection("jdbc:phoenix:"+init.zkAddr);
		byte[]pk = Bytes.toBytes(Utils.timeChange(init.scanStart, null));
		//rset = stmt.executeQuery("select * from LS12 LIMIT 10, Statement.RETURN_GENERATED_KEYS");
		if(init.scanLimit==-1)
			init.scanLimit = Long.MAX_VALUE;

		PreparedStatement statement = con.prepareStatement("select * from "+init.tableName + sql1);

		//statement.setBytes(1, pk);
		rset = statement.executeQuery();
		long count =0;

		while (rset.next()) {
			//System.out.println(rset.getLong("K"));
			//System.out.println(rset.getInt("dp"));
			count ++;
			donenum ++;
		}
		rset.close();
		statement.close();
		con.close();
		long insertendtime = System.currentTimeMillis();
		String ops = Utils.getresult(count,insertstarttime,insertendtime);
		System.out.println("get "+count+", total time: "+(insertendtime - insertstarttime)
				+" ms, average through: "+ ops+"/s");
	}
}
