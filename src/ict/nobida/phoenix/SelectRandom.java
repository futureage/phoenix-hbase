package ict.nobida.phoenix;
import ict.nobida.utils.Init;
import ict.nobida.utils.StatiThread;
import ict.nobida.utils.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

public class SelectRandom {
	class GetThread extends Thread {
		int prefix;
		long getCount;
		String tableName;
		public GetThread(String name, long count, int pre) {
			getCount = count;
			prefix = pre;
			tableName = name;
		}
		public void run() {
			ResultSet rset = null;
			Connection con;
			try {
				con = DriverManager.getConnection("jdbc:phoenix:"+init.zkAddr);
				if(init.scanLimit==-1)
					init.scanLimit = Long.MAX_VALUE;

				PreparedStatement pp = con.prepareStatement("select * from "+ tableName + " where k=?");
				long lrk = Utils.timeChange(init.scanStart, null);
				long k = 0;
				for(int j=0;j< getCount;++j) {
					k = lrk+init.rand.nextInt(Init.getRange);
					pp.setLong(1, k);
					rset = pp.executeQuery();
					if(rset.next())
						//System.out.println(rset.getLong(1));
						init.rtCount[prefix]++;
				}
				rset.close();
				pp.close();
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}
	public void mutiget(long totalCount) throws SQLException, InterruptedException {
		long insertstarttime = System.currentTimeMillis();
		try {
			Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		GetThread getts[] = new GetThread[Init.threadN];
		long perlimit=-1;
		long perlimit0=-1;
		if(totalCount > 0) {
			if(totalCount%Init.threadN > 0) {
				perlimit = totalCount/Init.threadN;
				perlimit0 = totalCount/Init.threadN+totalCount%Init.threadN;
			} else {
				perlimit = totalCount/Init.threadN;
				perlimit0=perlimit;
			}
		}
		for (int i = 0; i < getts.length; i++) {
			if(i==0)
				getts[i] = new GetThread(init.tableName, perlimit0, i);
			else
				getts[i] = new GetThread(init.tableName,perlimit, i);
			getts[i].setName("GetThread_" + i);
			getts[i].start();
		}
		StatiThread th = new StatiThread(init.rtCount, init.tt);
		th.start();

		for (int i = 0; i < getts.length; i++) {
			getts[i].join();
		}
		th.stop();
		long insertendtime = System.currentTimeMillis();
		String ops = Utils.getresult(totalCount, insertstarttime,insertendtime);
		System.out.println("get "+totalCount+", total time: "+(insertendtime - insertstarttime)
				+" ms, average through: "+ ops+"/s");
	}
	Init init = null;
	long scanStartNum;
	long scanEndNum;
	String filterStr;
	String scanResultPath=null;
	long scanLimitCount;

	public static void main(String[] args) throws SQLException, InterruptedException {
		SelectRandom select = new SelectRandom();
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
			} 
		}
		if(select.scanLimitCount == 0) {
			//System.out.println("please input the count");
			//return;
			select.scanLimitCount = select.init.scanLimit;
		}
		select.mutiget( select.scanLimitCount);
		return;
	}
	long donenum=0;

}
