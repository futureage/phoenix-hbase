package ict.nobida.hbase;

import ict.nobida.utils.Init;
import ict.nobida.utils.StatiThread;
import ict.nobida.utils.Utils;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class SPacketPut {
	 void put2Hbase2(HTableInterface metatable,byte[] rowkey,int pre, int ttt)  throws IOException {
		 rowkey[0] = (byte) pre;
		 long lrk = Utils.timeChange(init.scanStart, null)+ Init.counter.incrementAndGet();
		 System.arraycopy(Bytes.toBytes(lrk), 0, rowkey, 1, 8);
		 Put metaPut = new Put(rowkey);
		 metaPut.setWriteToWAL(init.wal);
		 //metaPut.setDurability(Durability.SKIP_WAL);
		 //metaPut.getWriteToWAL()
		 metaPut.add(Init.indexcf, Init.d, Bytes.toBytes(init.rand.nextInt(1000)));
		 metaPut.add(Init.indexcf, Init.dp, Bytes.toBytes(lrk));
		 metaPut.add(Init.indexcf, Init.s,Bytes.toBytes(init.rand.nextBoolean()));
		 metaPut.add(Init.indexcf, Init.sp,Bytes.toBytes(init.rand.nextDouble()));
		 metaPut.add(Init.packetcf, Init.z,Bytes.toBytes(Utils.getRandomString(init.datasize)));
		 metatable.put(metaPut);
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
				metatable = Init.pool.getTable(tableName);
				metatable.setAutoFlush(false);
				if(Init.writeBuffer!=0) {
					metatable.setWriteBufferSize(Init.writeBuffer);
				}
				for(long i=0;i<countPerThread;++i) {
					//if(i%100==0) {
						put2Hbase2(metatable, rowkey,prefixs,10);
					//} else {
					//	put2Hbase2(metatable, rowkey,prefixs,0);
					//}
					//put2HbaseV2(metatable, rowkey,prefixs);
					//donenum++;
					init.rtCount[prefixs]++;
					if(i%init.cacheCount == 0)
						metatable.flushCommits();
				}
				metatable.flushCommits();
				metatable.close();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}

	public void mutiput2(final String tableName,final int tt,long limit) throws InterruptedException{
		final PutThread putts[] = new PutThread[Init.preSplitsCount];
		long insertstarttime = System.currentTimeMillis();
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

		for (int i = 0; i < putts.length; i++) {
			if(i==0)
				putts[i] = new PutThread(tableName,i,perlimit0);
			else
				putts[i] = new PutThread(tableName,i,perlimit);
			putts[i].setName("PutThread_" + i);
			putts[i].start();
		}

		StatiThread sth = new StatiThread(init.rtCount, init.tt);
		sth.start();

		for (int i = 0; i < putts.length; i++) {
			putts[i].join();
		}
		sth.stop();
		long insertendtime = System.currentTimeMillis();
		String ops = Utils.getresult(Init.insertCount,insertstarttime,insertendtime);
		System.out.println("put "+Init.insertCount+", total time: "+(insertendtime - insertstarttime)
				+" ms, average through: "+ ops+"/s");
	} 
	Init init = null;
	public static void main(String args[]) {
		//  args[0] config path,
		SPacketPut bpp = new SPacketPut();
		bpp.init= new Init();
		bpp.init.init("putconf");
		try{
			bpp.mutiput2(bpp.init.tableName, bpp.init.tt, Init.insertCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

