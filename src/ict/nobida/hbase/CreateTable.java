package ict.nobida.hbase;

import ict.nobida.utils.Init;
import ict.nobida.utils.Utils;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
public class CreateTable {
	public boolean createTable(String tablename, byte[][] splits, String[] cf)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(Init.config);
		HTableDescriptor des = new HTableDescriptor(tablename);
		try {
			if (admin.tableExists(tablename)) {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			}
			for (int i =0; i < cf.length; i++) {
				HColumnDescriptor hcd = new HColumnDescriptor(cf[i]);
				//hcd.setCompressionType(Algorithm.SNAPPY);
				//hcd.setBloomFilterType(BloomType.ROW);
				//		    hcd.setBlocksize(2048);
				//hcd.setBloomFilterType(StoreFile.BloomType.ROWCOL);
				des.addFamily(hcd);
			}
			admin.createTable(des, splits);
			return true;
		} catch (TableExistsException e) {
			return false;
		} finally {
			admin.close();
		}
	}
	
	public boolean createTable1(HBaseAdmin admin, String tablename, byte[][] splits, String[] cf)
			throws IOException {
		HTableDescriptor des = new HTableDescriptor(tablename);
		try {
			if (admin.tableExists(tablename)) {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			}
			for (int i =0; i < cf.length; i++) {
				HColumnDescriptor hcd = new HColumnDescriptor(cf[i]);
				des.addFamily(hcd);
			}
			admin.createTable(des, splits);
			return true;
		} finally {
			admin.close();
		}
	}
	public boolean createIndexTable(String IndexName, byte[][] splits, String cf)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(Init.config);
		HTableDescriptor des = new HTableDescriptor(IndexName);
		try {
			if (admin.tableExists(IndexName)) {
				admin.disableTable(IndexName);
				admin.deleteTable(IndexName);
			}
			des.addFamily(new HColumnDescriptor(cf));
			HColumnDescriptor hcd = new HColumnDescriptor(cf);
			//hcd.setCompressionType(Algorithm.SNAPPY);
			admin.createTable(des, splits);
			return true;
		} finally {
			admin.close();
		}
	}
	public void ct(String tablename, int preSplitsCount, String[] cf) {
		byte[][] splits=null;
		byte[][] split1=null;
		byte[][] split2=null;
		byte[][] split3=null;
		byte[][] split4=null;
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(Init.config);
		} catch (MasterNotRunningException e1) {
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			e1.printStackTrace();
		}
		if(preSplitsCount> 1) {
			splits=Utils.getSplitV2(preSplitsCount);
			split1=Utils.getSplitidx(preSplitsCount);
			split2=Utils.getSplitidx(preSplitsCount);
			split3=Utils.getSplitidx(preSplitsCount);
			split4=Utils.getSplitidx(preSplitsCount);
		}
		try{
			createTable(init.tableName,splits,Init.cfs);
			if(init.idxt1 != null)
				createTable(init.tableName+"idx1",split1,Init.idxcf);
			if(init.idxt2 != null)
				createTable(init.tableName+"idx2",split2,Init.idxcf);
			if(init.idxt3 != null)
				createTable(init.tableName+"idx3",split3,Init.idxcf);
			if(init.idxt4 != null)
				createTable1(admin,init.tableName+"idx4",split4,Init.idxcf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	Init init = null;
	public static void main(String args[]) {
		CreateTable createT = new CreateTable();
		createT.init = new Init();
		createT.init.init("putconf");
		byte[][] splits=null;

		if(Init.preSplitsCount> 1) {
			splits=Utils.getSplitV2(Init.preSplitsCount);
		}
		createT.ct(createT.init.tableName,Init.preSplitsCount,Init.cfs);
	}
}

