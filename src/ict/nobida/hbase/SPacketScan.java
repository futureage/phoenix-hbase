package ict.nobida.hbase;

import ict.nobida.utils.Init;
import ict.nobida.utils.Utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.swing.text.html.parser.Entity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

//for single packet
public class SPacketScan {
	long donenum = 0;
	public long todiskbybytes(ScannerHeap sHeap,String filePath,long limit) {
		FileOutputStream fos = null;
		File file=null;
		BufferedOutputStream bos = null;
		long count =0;
		try {
			if(filePath!=null) {
				file = new File(filePath);
				fos = new FileOutputStream(file);
				bos = new BufferedOutputStream(fos);
			}
			Result rr=null;
			byte[] data=null;
			byte[] rk = new byte[8];
			if(limit==-1)
				limit = Long.MAX_VALUE;

			while((count < limit)) {
				rr = sHeap.next();
				if(rr==null) {
					break;
				}
				//System.arraycopy(rr.getRow(), 1, rk, 0, 8);
				if(filePath!=null) {
					data = rr.getColumnLatest(Init.packetcf, Init.z).getValue();
					bos.write(data, 0, data.length);
					if(limit%100 == 0)
						bos.flush();
				}
				count++;
				donenum++;
			}
			if(filePath!=null) {
				bos.flush();
				bos.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
		sHeap.close();
		return count;
	}

	public class MyPair<T1, T2> {
		protected T1 first = null;
		protected T2 second = null;
		public MyPair(T1 a, T2 b) {
			this.first = a;
			this.second = b;
		}
		public  <T1,T2> MyPair<T1,T2> newPair(T1 a, T2 b) {
			return new MyPair<T1,T2>(a, b);
		}
		public void setFirst(T1 a) {
			this.first = a;
		}
		public void setSecond(T2 b) {
			this.second = b;
		}
		public T1 getFirst() {
			return first;
		}
		public T2 getSecond() {
			return second;
		}
	}
	
	class Getss implements Callable<MyPair<ResultScanner, Result>> {  
		HTableInterface tableName = null;
		byte[] starttimeScan;
		byte[] endtimeScan;
		FilterList filters;
		long limit=1000;

		Getss(HTableInterface name, byte[] startkey,byte[] endkey, FilterList fs,long limit2) {
			this.tableName = name;
			starttimeScan=startkey;
			endtimeScan=endkey;
			filters = fs;
			if(limit<1000)
				this.limit=limit2;
		}
		public MyPair<ResultScanner, Result> call() throws Exception {
			ResultScanner rss = getScanners(tableName, starttimeScan, endtimeScan, filters, limit);
			Result rs = rss.next();
			if(rs != null)
				return new MyPair<ResultScanner, Result>(rss, rs);
			else {
				rss.close();
				return null;
			}
		}
	}
	public class ScannerHeap {
		private PriorityQueue<MyPair<Result, Integer> > heap = null;
		private Result current = null;
		private KVScannerComparator comparator = null;
		private ResultScanner[] rsArray = null;

		public ScannerHeap(List<? extends ResultScanner> scanners) throws IOException {
			this.comparator = new KVScannerComparator();
			if (!scanners.isEmpty()) {
				this.heap = new PriorityQueue<MyPair<Result, Integer> >(scanners.size(),
						this.comparator);
				this.rsArray = new ResultScanner[scanners.size()];
				//System.out.println(Bytes.toString(rsArray[0].next().getRow()));
				Result r=null;
				int i=0;
				for (ResultScanner scanner : scanners) {
					r = scanner.next();
					//System.out.println(Utils.toStringBinary(r.getRow(), 0, r.getRow().length));
					if (r != null) {
						this.heap.add(new MyPair<Result, Integer>(r,i));
						rsArray[i]=scanner;
						++i;
					} else {
						scanner.close();
					}
				}
				//this.current = next();
			}
		}

		public ScannerHeap(List<MyPair<ResultScanner, Result>> ssPair,boolean muti) throws IOException {
			this.comparator = new KVScannerComparator();
			if (!ssPair.isEmpty()) {
				this.heap = new PriorityQueue<MyPair<Result, Integer> >(ssPair.size(), this.comparator);
				this.rsArray = new ResultScanner[ssPair.size()];

				Result r=null;
				int i=0;
				for (MyPair<ResultScanner, Result> sp : ssPair) {
					if (sp.getSecond() != null) {
						this.heap.add(new MyPair<Result, Integer>(sp.getSecond(),i));
						rsArray[i]=sp.getFirst();
						++i;
					} else {
						sp.getFirst().close();
					}
				}
			}
			//this.current = next();
		}
		public void close() {
			if (this.heap != null) {
				for(int i=0;i<rsArray.length;++i) {
					if(rsArray[i]!=null)
						rsArray[i].close();
				}
			}
		}

		public List<Result> next(long limit) throws IOException{
			List<Result> res = new ArrayList<Result>();
			Result tmp =null;
			while((limit!=0)&&(heap.peek()!=null)) {
				if((tmp = next())!=null)
					res.add(tmp);
				limit--;
			}
			return res;
		}
		/**
		 * Fetches the top result from the priority queue,
		 */
		private Result next() throws IOException {
			MyPair<Result, Integer> rsPair = heap.poll();
			if (rsPair == null) {
				return null;
			}

			while (rsPair != null ) {
				if (rsPair.getFirst() != null) {
					Result curRes = rsPair.getFirst();
					MyPair<Result, Integer> nextEarliestPair = heap.peek();
					if (nextEarliestPair == null) {
						// The heap is empty. Return the only possible re.
						Result newr = rsArray[rsPair.getSecond()].next();
						if(newr!=null) {
							rsPair.setFirst(newr);
							heap.add(rsPair);
						} else{
							rsArray[rsPair.getSecond()].close();
							rsArray[rsPair.getSecond()]=null;
							rsPair.setFirst(null);
						}
						//heap.add(rsPair);
						return curRes;
					}
					// Compare the current re to the next re. We try to avoid
					// putting the current one back into the heap if possible.
					Result nextRes = nextEarliestPair.getFirst();
					// attention, in our project,comparator.compare()should never return 0
					if (nextRes == null || comparator.compare(rsPair, nextEarliestPair) <= 0) {
						// We already have the re with the earliest KV, so return it.
						Result newr = rsArray[rsPair.getSecond()].next();
						if(newr!= null) {
							rsPair.setFirst(newr);
							heap.add(rsPair);
						} else{
							rsArray[rsPair.getSecond()].close();
							rsArray[rsPair.getSecond()]=null;
							rsPair.setFirst(null);
						}
						return curRes;
					}
					heap.add(rsPair);
				} else {
					rsArray[rsPair.getSecond()].close();
					rsArray[rsPair.getSecond()]=null;
				}
				rsPair = heap.poll();
			}
			return null;
		}

		private class KVScannerComparator implements Comparator<MyPair<Result, Integer>> {
			public int compare(MyPair<Result, Integer> left, MyPair<Result, Integer> right) {
				byte[] leftRow=null;
				byte[] rightRow=null;
				leftRow = left.getFirst().getRow();
				rightRow = right.getFirst().getRow();

				byte[] leftTs= new byte[leftRow.length-1];
				byte[] rightTs = new byte[rightRow.length-1];
				System.arraycopy(leftRow,1,leftTs,0,leftRow.length-1);
				System.arraycopy(rightRow,1,rightTs,0,rightRow.length-1);
				//long re = Bytes.toLong(leftTs) - Bytes.toLong(rightTs);
				int re= Bytes.compareTo(leftTs, rightTs);
				if (re != 0) {
					return re;
				} else {
					// Since both the keys are exactly the same, 
					System.out.println("the two rowkey is equal..");
					return 0;
				}
			}
		}
	}

	public FilterList getFilterList(List<String> fs) {
		FilterList filterList = new FilterList();
		if(fs!=null) {
			for(String v:fs) {
				if(v!=null) {
					byte[] tmp=null;
					String [] ss=v.split(":");
					if(ss[0].equals(Bytes.toString(Init.d))) {
						tmp = Bytes.toBytes(Integer.parseInt(ss[1]));
					} else if(ss[0].equals(Bytes.toString(Init.dp))) {
						tmp = Bytes.toBytes(Long.parseLong(ss[1]));
					} else if(ss[0].equals(Bytes.toString(Init.s))) {
						tmp = Bytes.toBytes(Boolean.parseBoolean(ss[1]));
					} else if(ss[0].equals(Bytes.toString(Init.sp))) {
						tmp = Bytes.toBytes(Double.parseDouble(ss[1]));
					}
					filterList.addFilter(new SingleColumnValueFilter(Init.indexcf,Bytes.toBytes(ss[0]),
							CompareOp.EQUAL,tmp));
				}
			} 
		}
		return filterList;
	}

	public ResultScanner getScanners(HTableInterface hTable, byte[] startkey,
			byte[] endkey, FilterList filters,long caches) {
		Scan s =null;
		ResultScanner scanner = null;
		try {
			if(startkey==null) {
				s = new Scan();
			} else if(endkey==null) {
				s = new Scan(startkey);
			} else {
				s = new Scan(startkey,endkey);
			}
			if(caches<=init.cacheCount && caches!=-1) {
				s.setCaching((int)caches);
			} else{
				s.setCaching(init.cacheCount);
			}
			//s.addColumn(cf, z);
			if(filters!=null) {
				s.setFilter(filters);
			}
			scanner = hTable.getScanner(s);
		}catch(IOException e) {
			e.printStackTrace();
		}
		return scanner;
	}

	public void scan(final String tableName, long scanStarttime,
			long scanendtime, String filepath, String[] filters, int limit) throws InterruptedException, IOException {
		long starttime = System.currentTimeMillis();
		if(scanStarttime!=0 && scanendtime!=0 && scanendtime < scanStarttime) {
			System.out.println("scanStarttime shouldbe smaller than scanendtime");
			return;
		}
		long scanNum=0;
		List<ResultScanner> rsScanners=new ArrayList<ResultScanner>();
		HTableInterface hTable = init.pool.getTable(tableName);
		for (int i = 0; i < init.preSplitsCount; i++) {
			byte[] start = null;
			byte[] end = null;
			start = Utils.genRowkeyV2((byte)i,scanStarttime);
			end = Utils.genRowkeyV2((byte)i,scanendtime);
			//      FilterList flist = getFilterList(filters);
			ResultScanner rs = getScanners(hTable, start, end, null, limit);
			rsScanners.add(rs);
		}

		try {
			ScannerHeap sh = new ScannerHeap(rsScanners);
			scanNum = todiskbybytes(sh,filepath,limit);
		} catch (IOException e) {
			e.printStackTrace();
		}
		long endtime = System.currentTimeMillis();
		String ops = Utils.getresult(scanNum,starttime,endtime);
		System.out.println("total get "+scanNum +". the total time is: "+(endtime - starttime)+
				"ms, average throughput: "+ ops+"/s .");
	}

	public void mutiscan(final String tableName, long scanStarttime,
			long scanendtime, String filepath, List<String> filters, long limit) throws InterruptedException, IOException {
		long starttime = System.currentTimeMillis();
		if(scanStarttime!=0 && scanendtime!=0 &&scanendtime<scanStarttime) {
			System.out.println("scanStarttime shouldbe smaller than scanendtime");
			return;
		}
		long scanNum=0;
		List<MyPair<ResultScanner, Result>> rsList=new ArrayList<MyPair<ResultScanner, Result>>();
		ExecutorService pool = Executors.newFixedThreadPool(Init.preSplitsCount);
		final List<Future<MyPair<ResultScanner, Result>>> list = new ArrayList<Future<MyPair<ResultScanner, Result>>>();

		long perlimit=-1;
		long perlimit0=-1;
		if(limit>0) {
			if(limit%Init.preSplitsCount>0) {
				perlimit = limit/Init.preSplitsCount;
				perlimit0 = limit/Init.preSplitsCount+limit%Init.preSplitsCount;
			} else {
				perlimit = limit/Init.preSplitsCount;
				perlimit0=perlimit;
			}
		}
		FilterList flist=null;
		if(filters!=null && filters.size()!=0) {
			flist = getFilterList(filters);
		}
		for (int i = 0; i < Init.preSplitsCount; i++) {
			HTableInterface hTable = Init.pool.getTable(tableName);
			byte[] start = null;
			byte[] end = null;
			start = Utils.genRowkeyV2((byte)(i),scanStarttime);
			if(scanendtime != 0)
				end = Utils.genRowkeyV2((byte)(i),scanendtime);
			else { 
				if(i == (Init.preSplitsCount-1))
					end = null;//Utils.genRowkeyV3((byte)0xFF);
				else 
					end = Utils.genRowkeyV3((byte)i);
			}
			
			Callable<MyPair<ResultScanner, Result>> c = null;
			if(i==0) {
				c = new Getss(hTable, start, end, flist, perlimit0);
			} else {
				c = new Getss(hTable, start, end, flist, perlimit);
			}
			Future<MyPair<ResultScanner, Result>> f = pool.submit(c);
			list.add(f);
		}
		Thread th = new Thread() {
			long allnum = 0;
			long currentnum=0;
			public void run() {
				while(true){
					try {
						Thread.sleep(init.tt*1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					currentnum=donenum;
					donenum=0;
					allnum +=currentnum;
					System.out.println(init.tt+" sec: "+allnum+" operations; "+Utils.getresult2(currentnum,init.tt)+" current ops/sec;");
					currentnum=0;
				}
			}
		};
		th.start();
		for (Future<MyPair<ResultScanner, Result>> f : list) {
			try {
				MyPair<ResultScanner, Result> mp = (MyPair<ResultScanner, Result>)f.get();
				if(mp != null) {
					rsList.add(mp);
				} 
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			if(rsList.size() != 0) {
				ScannerHeap sh = new ScannerHeap(rsList,true);
				scanNum = todiskbybytes(sh,filepath,limit);
			} else {
				scanNum = 0;
			}
			th.stop();
			pool.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long endtime = System.currentTimeMillis();
		String ops = Utils.getresult(scanNum,starttime,endtime);
		System.out.println("total get "+scanNum +". the total time is: "+(endtime - starttime)+
				"ms, average throughput: "+ ops+"/s .");
	}

	Init init = null;
	long scanStartNum;
	long scanEndNum;
	String filterStr;
	String scanResultPath=null;
	long scanLimitCount;
	List<String> fList = null;
	public static void main(String args[]) throws InterruptedException {
		SPacketScan ScanData = new SPacketScan();
		//args[0] config path,
		// ScanData.init(args[0]);
		ScanData.init = new Init();
		ScanData.init.init("putconf");
		if(ScanData.init.scanLimit == 0) {
			System.out.println("total get 0");
			return;
		}
		String thenull = "null";
		ScanData.fList = new ArrayList<String>();
		for(int i=0;i<args.length;i++) {
			String[] tmparg = args[i].split("=");
			if(tmparg[0].equals("st")) {
				if(!tmparg[1].equals(thenull))
					ScanData.scanStartNum = Long.parseLong(tmparg[1]);
			} else if(tmparg[0].equals("et")) {
				if(!tmparg[1].equals(thenull))
					ScanData.scanEndNum = Long.parseLong(tmparg[1]);
			} else if(tmparg[0].equals("result")) {
				if(!tmparg[1].equals(thenull))
					ScanData.scanResultPath = tmparg[1];
			} else if(tmparg[0].equals("count")) {
				if(!tmparg[1].equals(thenull)) {
					ScanData.scanLimitCount = Long.parseLong(tmparg[1]);
					ScanData.init.scanLimit = ScanData.scanLimitCount;
				}
			} else if(tmparg[0].equals("s")) {
				if(!tmparg[1].equals(thenull)) {
					ScanData.fList.add("s:"+tmparg[1]);
				}
			} else if(tmparg[0].equals("d")) {
				if(!tmparg[1].equals(thenull)){
					ScanData.fList.add("d:"+tmparg[1]);
				}
			} else if(tmparg[0].equals("sp")) {
				if(!tmparg[1].equals(thenull)) {
					ScanData.fList.add("sp:"+tmparg[1]);
				}
			} else if(tmparg[0].equals("dp")) {
				if(!tmparg[1].equals(thenull)){
					ScanData.fList.add("dp:"+tmparg[1]);
				}
			}
		}

		long start=0;
		long end =0;
		if(ScanData.init.scanStart != null) {
			start = Utils.timeChange(ScanData.init.scanStart, null) + ScanData.scanStartNum;
			if(ScanData.scanEndNum != 0)
				end = Utils.timeChange(ScanData.init.scanStart, null) + ScanData.scanEndNum;
		}
		if(start==-1 || end ==-1)
			return;

		if((ScanData.init.scanReultsFilePath!=null)) {
			File file=null;
			file = new File(ScanData.init.scanReultsFilePath);
			if(file.exists()) {
				file.delete();
			}
		}
		try {
			ScanData.mutiscan(ScanData.init.tableName, start, end, ScanData.init.scanReultsFilePath, ScanData.fList, ScanData.scanLimitCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
