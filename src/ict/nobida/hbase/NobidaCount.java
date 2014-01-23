package ict.nobida.hbase;

import ict.nobida.utils.Init;
import ict.nobida.utils.Utils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

//for big packet
public class NobidaCount {
	public long getCount(ResultScanner rss, int index) {
		long count =0;
		Result rr=null;
		//byte[] bigpack=null;
		try {
			//long num1 = 0;
			while((rr = rss.next()) != null) {
				//bigpack = rr.getValue(ncInit.indexcf, ncInit.d);
//				int num = bigpack.length;
//				num1 = Bytes.toLong(bigpack);
				int ii = rr.getColumn(Init.indexcf, Init.d).size();
//				List<KeyValue> kVs = rr.getColumn(Init.indexcf, Init.d);
//				for(int i =0; i< kVs.size(); ++i){
//					System.out.println(Bytes.toInt(kVs.get(i).getValue()));
//				}
				count += ii;
				ncInit.rtCount[index] += ii;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return count;
	}

	public ResultScanner getScanners(HTableInterface hTable, byte[] startkey,
			byte[] endkey, long caches) {
		Scan s =null;
		ResultScanner scanner = null;
		try {
			if(startkey == null) {
				s = new Scan();
			} else if(endkey == null) {
				s = new Scan(startkey);
			} else {
				s = new Scan(startkey, endkey);
			}
			s.setMaxVersions();
			if(caches <= ncInit.cacheCount && caches>=0) {
				s.setCaching((int)caches);
			} else {
				s.setCaching(ncInit.cacheCount);
			}
			//s.setFilter(new org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter());
			s.addColumn(ncInit.indexcf, ncInit.d);
			scanner = hTable.getScanner(s);
		}catch(IOException e) {
			e.printStackTrace();
		}
		return scanner;
	}
	class Getss implements Callable<Long> {
		HTableInterface tableName = null;
		byte[] starttimeScan = null;
		byte[] endtimeScan = null;
		long limit=1000;
		int index =-1;

		Getss(HTableInterface name, byte[] startkey,byte[] endkey, long caches,int ind) {
			this.tableName = name;
			starttimeScan=startkey;
			endtimeScan=endkey;
			this.limit=caches;
			index = ind;
		}
		public Long call() throws Exception {
			ResultScanner rss = getScanners(tableName, starttimeScan, endtimeScan, limit);
			long pernum=0;
			if(rss!=null) {
				pernum= getCount(rss, index);
			}     
			return pernum;
		}
	}  

	public void mutiscan(final String tableName) throws InterruptedException, IOException {
		long starttime = System.currentTimeMillis();
		
		long scanNum=0;
		long scanStarttime=0;
		long scanendtime=0;
		ExecutorService exePool = Executors.newFixedThreadPool(Init.preSplitsCount);
		CompletionService<Long> completionServcie = new ExecutorCompletionService<Long>(exePool);
		Callable<Long> c = null;
		long cacheLimit=ncInit.cacheCount;

		for (int i = 0; i < Init.preSplitsCount; i++) {
			HTableInterface hTable = Init.pool.getTable(tableName);
			byte[] start = null;
			byte[] end = null;
			start = Utils.genRowkeyV2((byte)(i),scanStarttime);
			if(scanendtime !=0)
				end = Utils.genRowkeyV2((byte)(i),scanendtime);
			else{ 
				if(i == (Init.preSplitsCount-1))
					end = null;//Utils.genRowkeyV3((byte)0xFF);
				else 
					end = Utils.genRowkeyV3((byte)i);
			}
				c = new Getss(hTable, start, end, cacheLimit,i);
			completionServcie.submit(c);
		}

		Thread th = new Thread() {
			long allnum = 0;
			long currentnum=0;
			public void run() {
				while(true) {
					try {
						Thread.sleep(ncInit.tt*1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for(int i =0; i< ncInit.rtCount.length;++i) {
						currentnum+=ncInit.rtCount[i];
						ncInit.rtCount[i]=0;
					}
					allnum +=currentnum;
					System.out.println(ncInit.tt+" sec: "+allnum+" operations; "+Utils.getresult2(currentnum,ncInit.tt)+" current ops/sec;");
					currentnum=0;
				}
			}
		};
		th.start();

		for (int i=0; i<Init.preSplitsCount;i++) {
			try {
				long pn = (Long)completionServcie.take().get();
				scanNum+=pn;
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		th.stop();
		exePool.shutdown();
		long endtime = System.currentTimeMillis();
		String ops = Utils.getresult(scanNum,starttime,endtime);
		if((endtime - starttime)/1000>10) {
			System.out.println("total get "+scanNum +". time is: "+
					(endtime - starttime)/1000+" s, average throughput: "+ ops+"/s");
		} else {
			System.out.println("total get "+scanNum +". time is: "+
					(endtime - starttime)+" ms, average throughput: "+ ops+"/s");
		}
		return ;
	}

	String scanStartTime;
	String scanEndTime;

	Init ncInit = null;
	public static void main(String args[]) throws InterruptedException {
		NobidaCount bps = new NobidaCount();
		//args[0] config path,
		//bps.init("scanconf");
		bps.ncInit = new Init();
		if(args==null || args.length < 2) {
			System.out.println("please input the conf name and table name");
			return;
		}
		bps.ncInit.init(args[0]);

		try {
			bps.mutiscan(args[1]);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


