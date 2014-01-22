package ict.nobida.hbase;

import ict.nobida.utils.Init;
import ict.nobida.utils.StatiThread;
import ict.nobida.utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

//for big packet
public class SScanWithIndex {

	public ResultScanner getScanners(HTableInterface hTable, byte[] startkey,
			byte[] endkey,FilterList filterList, long caches) {
		Scan s =null;
		ResultScanner scanner = null;
		try {
			if(startkey==null ) {
				s = new Scan();
			} else if(endkey==null) {
				s = new Scan(startkey);
			} else {
				s = new Scan(startkey,endkey);
			}
			if(caches<=init.cacheCount && caches>=0){
				s.setCaching((int)caches);
			} else{
				s.setCaching(init.cacheCount);
			}

			if(filterList!=null)
				s.setFilter(filterList);
			scanner = hTable.getScanner(s);
		}catch(IOException e) {
			e.printStackTrace();
		}
		return scanner;
	}

	public void getfromrt(HTableInterface hTable, byte[] key) {
	    //HashMap<String,byte[]> result=new HashMap<String, byte[]>();
	    Result r = null;
	      try {
	      	//Get g = new Get(Bytes.toBytes(key));
	      	Get g = new Get(key);
//	        if (fields == null) {
//	          for(int i=0;i<init.cfs.length;++i){
//	            g.addFamily(Bytes.toBytes(init.cfs[i]));
//	      	  }
//	        } else {
//	          for (int i =0; i<fields.length;++i) {
//	        	String[] ff = fields[i].split(":");
//	          	g.addColumn(Bytes.toBytes(ff[0]), Bytes.toBytes(ff[1]));
//	          }
//	        }
	        r = hTable.get(g);
	        if(r==null)
	          System.out.println("null:"+Utils.toStringBinary(key, 0, key.length));
	        else {
	        	 for (KeyValue kv : r.raw()) {
	       	      //result.put(Bytes.toString(kv.getQualifier()),kv.getValue());
	       	    }
			}
	      } catch (Exception e) {
	          System.err.println("Error doing get: "+e);
	          return ;
	      }
	   
	    return;
	  }
	public long topKtoMap(ResultScanner rss,long limit, int index,boolean onlyCount) {
		long count =0;
		try {
			Result rr=null;
			byte[] rowKey=null;
			byte[] rrk = null;
			HTableInterface rht = null;
			if(!onlyCount) {
				 rrk = new byte[9];
				rht = init.pool.getTable(init.tableName);
			}
			if(limit<0)
				limit = Long.MAX_VALUE;
			while((count < limit)) {
				rr = rss.next();
				if(rr==null) {
					break;
				}
				rowKey = rr.getRow();
				if(!onlyCount) {
				  System.arraycopy(rowKey,4, rrk, 0, 9);
				  getfromrt(rht,rrk);
				}
				count++;
				if(count>=limit){
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(rss!=null)
				rss.close();
		}
		return count;
	}

	class GetTarget implements Callable<Long> {  
		HTableInterface tableName = null;
		byte[] starttimeScan;
		byte[] endtimeScan;
		FilterList filters;
		long limit=1000;
		int index =-1;
		boolean onlycount = true;

		GetTarget(HTableInterface name, byte[] startkey,byte[] endkey, long caches,int ind,boolean gd) {
			this.tableName = name;
			starttimeScan=startkey;
			endtimeScan=endkey;
			this.limit=caches;
			index = ind;
			onlycount = gd;
		}
		public Long call() throws Exception {
			ResultScanner rss = getScanners(tableName, starttimeScan, endtimeScan, null, limit);
			long pernum=0;
			if(rss!=null){
				pernum= topKtoMap(rss, limit,index,onlycount);
			}     
			return pernum;
		}
	}  


	byte[] dks = {(byte) 0x00,(byte) 0x00,(byte) 0x00,(byte) 0x00};
	byte[] dke = {(byte) 0xff,(byte) 0xff,(byte) 0xff,(byte) 0xff};
	
	public void topK(int sk, long limit, long st,long et, int idxnum) {
		long starttime = System.currentTimeMillis();
		ExecutorService exePool = Executors.newFixedThreadPool(init.prefixCount);
		CompletionService<Long> completionServcie = new ExecutorCompletionService<Long>(exePool);
		Callable<Long> c = null;
		long perlimit=-1;
		long perlimit0=-1;
		if(limit>0) {
			if(limit%init.prefixCount>0) {
				perlimit = limit/init.prefixCount;
				perlimit0 = limit/init.prefixCount+limit%init.prefixCount;
			} else {
				perlimit = limit/init.prefixCount;
				perlimit0=perlimit;
			}
		}
		
		for (int i = 0; i < init.prefixCount; i++) {
			byte[] start = null;
			byte[] end = null;
			start = Utils.genRowkeyforidxscan((byte)(i+0),sk,(byte)0,st);
			end = Utils.genRowkeyforidxscan((byte)(i+0),sk,(byte)0xFF,et);
			HTableInterface hTable = init.pool.getTable(init.tableName+"idx"+idxnum);
			if(i==0)
				c = new GetTarget(hTable, start, end, perlimit0,i,init.onlyCount);
			else 
				c = new GetTarget(hTable, start, end, perlimit,i,init.onlyCount);
			completionServcie.submit(c);
		}

		StatiThread th = null;
		th = new StatiThread(init.rtCount, init.tt);
		th.start();

		long scanNum = 0;
		for (int i=0; i<init.prefixCount;i++) {
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
	}
	
	Init init = null;
	public static void main(String args[]) throws InterruptedException {
		SScanWithIndex bps = new SScanWithIndex();
		bps.init = new Init();
		bps.init.init("putconf");

		if(args==null || args.length == 0) {
			System.out.println("please input target value");
			return;
		}
		bps.init.curridxnum = Integer.parseInt(args[0]);
		int target = Integer.parseInt(args[1]);
		
		long start=0;
		long end =0;
		if (bps.init.scanStart!=null)
			start = Utils.timeChange(bps.init.scanStart, null);
		if(bps.init.scanEnd!= null)
			end = Utils.timeChange(bps.init.scanEnd, null);
		if(start ==-1 || end ==-1)
			return;
		
		try {
			if(bps.init.curridxnum==0)
			{
				System.out.println("please input the index number");
				return;
			}
			bps.topK(target, bps.init.scanLimit, start, end,bps.init.curridxnum);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}