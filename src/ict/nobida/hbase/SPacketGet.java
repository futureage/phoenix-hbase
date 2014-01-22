package ict.nobida.hbase;
import ict.nobida.utils.Init;
import ict.nobida.utils.StatiThread;
import ict.nobida.utils.Utils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class SPacketGet {
	/**
	 * Read a record from the hbase. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	public Result getOneRow(HTableInterface hTable, byte[] key, String[] fields) {
		try {
			Get g = new Get(key);
			//			if (fields != null) {
			//				for (int i =0; i<fields.length;++i) {
			//					String[] ff = fields[i].split(":");
			//					g.addColumn(Bytes.toBytes(ff[0]), Bytes.toBytes(ff[1]));
			//				}
			//			}
			//r = hTable.get(g);
			//hTable.exists(g);
			if(hTable.exists(g))
				return hTable.get(g);
			else {
				return null;
			}
		} catch (Exception e) {
			System.err.println("Error doing get: "+e);
			return null;
		}
		//System.out.println(String.valueOf(r.getValue(Init.indexcf, Init.d)));
		//Utils.toStringBinary(r.getValue(Init.indexcf, Init.d),0,4);
		//		for (KeyValue kv : r.raw()) {
		//			Utils.toStringBinary(kv.getQualifier(),0,kv.getQualifier().length);
		//			result.put(Bytes.toString(kv.getQualifier()),kv.getValue());
		//		}

	}

	public void mutiGets(final String tableName, int getCount, final int range,final String[] fields) {
		final int gets = getCount/Init.threadN;
		System.out.println("start gettest: "+getCount+" rows");
		final CountDownLatch countDownLatch = new CountDownLatch(Init.threadN);  
		long starttime = System.currentTimeMillis();
		for (int i = 0; i < Init.threadN; i++) {
			Thread th = new Thread() {
				HTableInterface htable = Init.pool.getTable(tableName);
				public void run() {
					try {
						byte[]rowkey = new byte[9];
						long lrk = Utils.timeChange(init.scanStart, null);
						for(int j=0;j< gets;++j) {
							rowkey[0] = (byte) init.rand.nextInt(Init.preSplitsCount);
							System.arraycopy(Bytes.toBytes(lrk+init.rand.nextInt(range)), 0, rowkey, 1, 8);
							getOneRow(htable,rowkey,fields);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}finally{
						try {
							htable.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					countDownLatch.countDown();
				}
			};
			th.start();
		}

		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long endtime = System.currentTimeMillis();
		String ops = Utils.getresult(getCount,starttime,endtime);
		System.out.println("randam get "+getCount +", the total time is: "+(endtime - starttime)+
				"ms, average throughput: "+ ops+"/s "); 
	}

	public void singleGets(final String tableName, int getCount, int range,String[] fields){
		long starttime = System.currentTimeMillis();
		System.out.println("start gettest.");
		HTableInterface hTable = Init.pool.getTable(tableName);
		for (int i = 0; i < getCount; i++) {
			byte[]rowkey = new byte[9];
			long lrk = Utils.timeChange(init.scanStart, null);
			rowkey[0] = (byte) init.rand.nextInt(Init.preSplitsCount);
			System.arraycopy(Bytes.toBytes(lrk+init.rand.nextInt(range)), 0, rowkey, 1, 8);
			getOneRow(hTable,rowkey,null);
		}
		long endtime = System.currentTimeMillis();
		String ops = Utils.getresult(getCount,starttime,endtime);
		System.out.println("randam get "+getCount +", the total time is: "+(endtime - starttime)+
				"ms, average throughput: "+ ops+"/s ");
	}

	class GetThread extends Thread {
		public long donenum =0;
		String tableName = null;
		int range = 0;
		String[] fields;
		long countPerThread = 0;
		int pre;

		GetThread(String name,int rangenum,String[] scanfields, long count, int prefix) {
			this.tableName = name;
			this.range = rangenum;
			fields = scanfields;
			countPerThread = count;
			pre = prefix;
		}

		public void run() {
			HTableInterface hTable = Init.pool.getTable(tableName);
			//final long countPerThread = Init.getCount;
			try {
				byte[]rowkey = new byte[9];
				long lrk = Utils.timeChange(init.scanStart, null);
				Result result = null;
				byte pp = (byte)0;
				for(int j=0;j< countPerThread;++j) {
					long k = lrk+init.rand.nextInt(range);
					do {
						if(pp > (byte)(Init.preSplitsCount-1))
							break;
						rowkey[0] = pp;
						System.arraycopy(Bytes.toBytes(k), 0, rowkey, 1, 8);
						result = getOneRow(hTable,rowkey,fields);
						pp = (byte)(pp +1);
					} while(result == null);
					if(result != null)
					{  byte[] bb = result.getRow();
					//System.out.println(pp);
					//System.out.println(Utils.toStringBinary(bb, 0, bb.length));
					} else {
						System.out.println(pp);
					}
					//donenum++;
					init.rtCount[pre]++;
					pp = (byte)0;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void mutiget2(final String tableName, long getCount, 
			final int range,final String[] fields,final int tt) throws InterruptedException{
		final GetThread getts[] = new GetThread[Init.threadN];
		long starttime = System.currentTimeMillis();

		long countPerThread = getCount/getts.length;
		for (int i = 0; i < getts.length; i++) {
			getts[i] = new GetThread(tableName,range,fields,countPerThread,i);
			getts[i].setName("GetThread_" + i);
			getts[i].start();
		}
		StatiThread sth = new StatiThread(init.rtCount, init.tt);
		sth.start();
		for (int i = 0; i < getts.length; i++) {
			getts[i].join();
		}
		sth.stop();
		long endtime = System.currentTimeMillis();
		String ops = Utils.getresult(getCount,starttime,endtime);
		System.out.println("randam get "+getCount +", the total time is: "+(endtime - starttime)+
				"ms, average throughput: "+ ops+"/s "); 
	}
	Init init = null;
	public static void main(String args[]) throws InterruptedException {
		SPacketGet sGet = new SPacketGet();
		sGet.init = new Init();
		//args[0] config path,
		sGet.init.init(args[0]);

		String fields = sGet.init.getField;
		String[] ffSet = null;
		if(fields!=null)
			ffSet = fields.split(",");
		sGet.mutiget2(sGet.init.tableName, Init.getCount, Init.getRange, ffSet, sGet.init.tt);
		//sGet.singleGets(sGet.init.tableName, Init.getCount, Init.getRange, ffSet, sGet.init.tt);
	}
}

