package ict.nobida.hbase;

import ict.nobida.utils.Init;
import ict.nobida.utils.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class SPutWithIndex {
	byte aa = (byte)1;
	int dd = 10;
	long ddp = 100L;
	Boolean ss = true;
	double ssp = 9.9;
	String zz= Utils.getRandomString(1024);
	byte[] bytetu = Bytes.toBytes(aa);
	byte[]  byteipd = Bytes.toBytes(dd);
	byte[]  bytedport = Bytes.toBytes(ddp);
	byte[]  byteips = Bytes.toBytes(ss);
	byte[]  bytesport = Bytes.toBytes(ssp);
	byte[] bytez = Bytes.toBytes(zz);

	public void put2HbaseV2(HTableInterface metatable,byte[] rowkey,int pre,
			HTableInterface idx1,HTableInterface idx2,HTableInterface idx3,HTableInterface idx4, 
			int ttt)  throws IOException {
		rowkey[0] = (byte) pre;
		long lrk = Utils.timeChange(init.scanStart, null)+ init.counter.incrementAndGet();
		System.arraycopy(Bytes.toBytes(lrk), 0, rowkey, 1, 8);
		Put metaPut = new Put(rowkey);
		metaPut.setWriteToWAL(false);
		int a1 = ttt;
		int a2 = init.rand.nextInt(1000000);
		int a3 = init.rand.nextInt(100000);
		int a4 = init.rand.nextInt(10000);

		metaPut.add(Init.indexcf, Init.a,Bytes.toBytes(a1));
		metaPut.add(Init.indexcf, Init.d,Bytes.toBytes(a2));
		metaPut.add(Init.indexcf, Init.dp,Bytes.toBytes(a3));
		metaPut.add(Init.indexcf, Init.s,Bytes.toBytes(a4));
		metaPut.add(Init.indexcf, Init.sp,Bytes.toBytes(lrk));
		metaPut.add(Init.packetcf, Init.z,bytez);
		metatable.put(metaPut);
		if(init.idxt1!=null) {
			putIndex(metaPut,pre, a1, rowkey, idx1);
			if(init.idxt2!=null) {
				putIndex(metaPut,pre, a2, rowkey, idx2);
				if(init.idxt3!=null) {
					putIndex(metaPut,pre, a3, rowkey, idx3);
					if(init.idxt4!=null) {
						putIndex(metaPut,pre, a4, rowkey, idx4);
					}
				}
			}
		}
	}

	public void putIndex(Put mainPut,int i, int value, byte[]rk, HTableInterface idxt) throws IOException {
		if(!mainPut.isEmpty() ) {
			byte[] idxrk = Utils.genRowkeyforidx((byte)i, value, rk);
			Put indexPut = new Put(idxrk);
			indexPut.setWriteToWAL(false);
			indexPut.add(Init.indexcf, Bytes.toBytes("k"),null);
			idxt.put(indexPut);
		}
	}

	class PutThread extends Thread {
		public long donenum =0;
		String tableName = null;
		int prefixs = 0;
		byte[] rowkey = new byte[9];
		long countPerThread = 0;

		PutThread(String name,int theprefixs,long count) {
			this.tableName = name;
			prefixs = theprefixs;
			countPerThread = count;
		}

		public void run() {
			try {
				HTableInterface metatable =null;
				metatable = init.pool.getTable(tableName);
				metatable.setAutoFlush(false);
				HTableInterface idx1 = null;
				HTableInterface idx2 = null;
				HTableInterface idx3 =null;
				HTableInterface idx4 =null;
				if(init.idxt1!=null) {
					idx1 = init.pool.getTable(tableName+"idx1");
					idx1.setAutoFlush(false);
					if(init.idxt2!=null) {
						idx2 = init.pool.getTable(tableName+"idx2");
						idx2.setAutoFlush(false);
						if(init.idxt3!=null) {
							idx3 = init.pool.getTable(tableName+"idx3");
							idx3.setAutoFlush(false);
							if(init.idxt4!=null) {
								idx4 = init.pool.getTable(tableName+"idx4");
								idx4.setAutoFlush(false);
							}
						}
					}
				}

				if(init.writeBuffer!=0) {
					metatable.setWriteBufferSize(init.writeBuffer);
				}
				for(long i=0;i<countPerThread;++i) {
					if(i%100==0) {
						put2HbaseV2(metatable, rowkey,prefixs,idx1,idx2,idx3,idx4,10);
					} else {
						put2HbaseV2(metatable, rowkey,prefixs,idx1,idx2,idx3,idx4,0);
					}
					donenum++;
					if(i%1000==0){
						metatable.flushCommits();
						if(init.idxt1!=null) {
							idx1.flushCommits();
							if(init.idxt2!=null) {
								idx2.flushCommits();
								if(init.idxt3!=null) {
									idx3.flushCommits();
									if(init.idxt4!=null) {
										idx4.flushCommits();
									}
								}
							}
						}
					}
				}
				metatable.flushCommits();
				if(init.idxt1!=null) {
					idx1.flushCommits();
					if(init.idxt2!=null) {
						idx2.flushCommits();
						if(init.idxt3!=null) {
							idx3.flushCommits();
							if(init.idxt4!=null) {
								idx4.flushCommits();
							}
						}
					}
				}
				
				metatable.close();
				if(init.idxt1!=null) {
					idx1.close();
					if(init.idxt2!=null) {
						idx2.close();
						if(init.idxt3!=null) {
							idx3.close();
							if(init.idxt4!=null) {
								idx4.close();
							}
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}

	public void mutiput2(final String tableName,final int tt,long limit) throws InterruptedException{
		final PutThread putts[] = new PutThread[init.preSplitsCount];
		long insertstarttime = System.nanoTime();

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

		for (int i = 0; i < putts.length; i++) {
			if(i==0)
				putts[i] = new PutThread(tableName,i,perlimit0);
			else 
				putts[i] = new PutThread(tableName,i,perlimit);
			putts[i].setName("ScanThread_" + i);
			putts[i].start();
		}

		Thread th = new Thread() {
			long allnum = 0;
			long currentnum=0;
			public void run() {
				while(true){
					try {
						Thread.sleep(tt*1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for(int i =0; i< putts.length;++i) {
						currentnum+=putts[i].donenum;
						putts[i].donenum=0;
					}
					allnum +=currentnum;
					System.out.println(tt+" sec: "+allnum+" operations; "+Utils.getresult2(currentnum,tt)+" current ops/sec;");
					currentnum=0;
				}
			}
		};
		th.start();

		for (int i = 0; i < putts.length; i++) {
			putts[i].join();
		}
		th.stop();
		long insertendtime = System.nanoTime();
		String ops = Utils.getresult(init.insertCount,insertstarttime/1000/1000,insertendtime/1000/1000);
		System.out.println("put "+init.insertCount+", total time: "+(insertendtime - insertstarttime)/1000/1000
				+" ms, average through: "+ ops+"/s");
	} 
	Init init = null;

	public static void main(String args[]) {
		//  args[0] config path,
		SPutWithIndex bpp = new SPutWithIndex();
		bpp.init= new Init();
		bpp.init.init("putconf");
		try{
			bpp.mutiput2(bpp.init.tableName,bpp.init.tt,bpp.init.insertCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

