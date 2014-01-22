package ict.nobida.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import ict.nobida.utils.Init;

public class NobidaClear {
	public void clear() {
		try {
			HBaseAdmin admin = new HBaseAdmin(Init.config);
			HTableDescriptor[] htlist= admin.listTables();
			for(int i=0; i< htlist.length; ++i) {
				byte[] tb = htlist[i].getName();
				admin.disableTable(tb);
				admin.deleteTable(tb);
			}
			admin.close();
			System.out.println("done");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	Init init = null;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		NobidaClear clear = new NobidaClear();
		clear.init = new Init();
		clear.init.init("putconf");
		clear.clear();
	}

}
