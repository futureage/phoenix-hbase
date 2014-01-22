package ict.nobida.hbase;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class BigPacketPut {
  public static HTablePool pool = null;
  public static  int commitCount = 100; //每次提交时录入的行数
  public static long writeBuffer = 1024*1024*6;
  public  String tableName=null;
  public  String indextable=null;
  
  public static final String HBASE_CONF ="hbase.config.path";
  public static final String THREAD_NUM ="thread.num";
  public static final String START_NUM ="start.instert.num";
  public static final String INSERT_COUNT = "insert.count";
  public static final String PRE_SPLITS_COUNT = "pre.splits.count";
  public static final String TABLE_NAME ="table.name";
  public static final String COMMIT_COUNT="commit.count";
  public static final String TT_STRING="Statistics.interval";
  public static final String WAL = "write.ahead.log";
  public static final String DATA_SIZE = "data.size";
  public static final String PREFIX_COUNT = "prefix.count.of.the.process";
  public static final String PREFIX_START_NUM = "prefix.start.num.of.the.process";
  public static final String SCAN_START_TIME= "scan.start.time";
  public static final String SCAN_END_TIME= "scan.end.time";
  public static final String BIG_PACKET_SIZE="big.packet.size";
	  
	  FileInputStream fis;
	  Properties pro = new Properties();
	  String hbaseConf = null; 
	  static int threadN=0;
	  public static int preSplitsCount=0;
	  public static Configuration config = new Configuration();//HBaseConfiguration.create();//获得Hbase配置参数
	  static int startNum=0;
	  static int insertCount=500;
	  static int getCount=0;
	  static int getRange=0;
	  String getField;
	  String cfstring;
	  int tt =10;
	  boolean wal=true;
	  int datasize=50;
	  int prefixCount=0;
	  int prefixStartNum=0;
	  String filters;
	  String scanStart;
	  String scanEnd;
	  String scanReultsFilePath;
	  int scanLimit=1000; 
	  boolean resultSort=true;
	  public int bigPackSize=1;
	  
	  static AtomicInteger counter= null;
	  Random rand = null;
	  
	  static public String[] cfs = {"b","d"}; //b mean basic ,d means data
	  static public byte[] indexcf = Bytes.toBytes("b");
	  static public byte[] packetcf = Bytes.toBytes("d");
	  static public byte[] a = Bytes.toBytes("a");
	  static public byte[] d = Bytes.toBytes("d");
	  static public byte[] dp = Bytes.toBytes("dp");
	  static public byte[] s = Bytes.toBytes("s");
	  static public byte[] sp = Bytes.toBytes("sp");
	  static public byte[] z = Bytes.toBytes("z");
	  
	  
	  public void init(String path) {
	    long seed = System.nanoTime();
		//long seed = 1;
	    rand = new Random(seed);
	  
	    String currdir =null;
	    String confpath = null;
	    try {
	      if(!path.startsWith(File.separator)) {
	        currdir = System.getProperty("user.dir");
	        confpath = currdir+File.separator+path;
	      } else{
	        confpath = path;
	      }
	      fis = new FileInputStream(confpath);
	      pro.load(fis);
	    } catch (FileNotFoundException e) {
	    	e.printStackTrace();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	    hbaseConf = pro.getProperty(HBASE_CONF);
	    config.addResource(new Path(hbaseConf));
	    pool = new HTablePool(config, 200);
	      
	    threadN = Integer.parseInt(pro.getProperty(THREAD_NUM));
	    startNum = Integer.parseInt(pro.getProperty(START_NUM));
	    counter = new AtomicInteger(startNum);
	    insertCount = Integer.parseInt(pro.getProperty(INSERT_COUNT));
	    preSplitsCount = Integer.parseInt(pro.getProperty(PRE_SPLITS_COUNT));
	    tableName = pro.getProperty(TABLE_NAME);
	    String tmpcommit = pro.getProperty(COMMIT_COUNT);
	    if(tmpcommit!=null)
	    	commitCount = Integer.parseInt(pro.getProperty(COMMIT_COUNT));
	     String tmptt= pro.getProperty(TT_STRING);
	    if(tmptt!=null)
	    	tt = Integer.parseInt(tmptt);
	    String tmpwal = pro.getProperty(WAL);
	    if(tmpwal!=null)
	    	wal=Boolean.parseBoolean(tmpwal);
	    String tmpdatasize = pro.getProperty(DATA_SIZE);
	    if(tmpdatasize!=null)
	    	datasize=Integer.parseInt(tmpdatasize);
	    
	    String tmpc=pro.getProperty(PREFIX_COUNT);
	    if (tmpc!=null)
	    	prefixCount=Integer.parseInt(tmpc);
	    String tmps=pro.getProperty(PREFIX_START_NUM);
	    if (tmps!=null)
	    	prefixStartNum=Integer.parseInt(tmps);
	    String tmpss=pro.getProperty(SCAN_START_TIME);
	    if (tmpss!=null&&(!tmpss.equals("")))
	    	scanStart=tmpss.substring(1,tmpss.length()-1);
	    System.out.println(scanStart);
	    String tmpse=pro.getProperty(SCAN_END_TIME);
	    if (tmpse!=null&&(!tmpse.equals("")))
	    	scanEnd=tmpse;
	    String tmpbps=pro.getProperty(BIG_PACKET_SIZE);
	    if (tmpbps!=null&&(!tmpbps.equals("")))
	        bigPackSize = Integer.parseInt(tmpbps);
	   // Properties prop = new Properties();
	   // prop.setProperty("log4j.rootLogger", "ERROR");
	   // PropertyConfigurator.configure(prop);
	   tmp = Bytes.toBytes(Utils.getRandomString(datasize));
	  }
	
  static AtomicInteger putCounter;
  static byte[] tmp;
  public byte[] indextmp = Bytes.toBytes(Utils.getRandomString(100));
  static byte[] bb = Bytes.toBytes((byte)6);
  public byte[] spp = Bytes.toBytes((short)666);
  public byte[] dpp = Bytes.toBytes((short)66);
  static long basistime=0;
  
  //for big packet
  byte[] tmpsip = Utils.convertip(("1.0.0.1"));
  byte[] tmpdip = Utils.convertip(("1.0.0.2"));
  byte[] tmpsport = Bytes.toBytes((short)1);
  byte[] tmpdport = Bytes.toBytes((short)2);
  byte[] tmptu = Bytes.toBytes((byte)6);
  byte[] onlysip1 = Utils.convertip(("251.0.0.1"));
  byte[] onlysip2 = Utils.convertip(("251.0.0.2"));
  byte[] onlydip1 = Utils.convertip(("252.0.0.1"));
  byte[] onlydip2 = Utils.convertip(("252.0.0.2"));
  byte[] onlysport = Bytes.toBytes((short)1001);
  byte[] onlydport = Bytes.toBytes((short)1002);
  byte[] onlytu = Bytes.toBytes((byte)17);
  
  byte[] bytetu = Bytes.toBytes(1);
  byte[]  byteipd = Bytes.toBytes(10);
  byte[]  bytedport = Bytes.toBytes(1000L);
  byte[]  byteips = Bytes.toBytes('a');
  byte[]  bytesport = Bytes.toBytes(9.1);
  byte[] byteokornot = Bytes.toBytes(true);
  public void put2HbaseV2(HTableInterface metatable,boolean wal,int pre,byte[] rowkey,
		  boolean only)  throws IOException {
	rowkey[0] = (byte) pre;
	System.arraycopy(Bytes.toBytes(Utils.timeChange(scanStart, null)+
			(long)counter.incrementAndGet()), 0, rowkey, 0, 8);
	
    Put metaPut = new Put(rowkey);
    metaPut.setWriteToWAL(wal);
//    byte[] byteips = new byte[bigPackSize*4];
//    byte[] byteipd = new byte[bigPackSize*4];
//    byte[] bytesport = new byte[bigPackSize*4];
//    byte[] bytedport = new byte[bigPackSize*4];
//    byte[] bytetu = new byte[bigPackSize*4];
//    
//    for(int i=0;i<bigPackSize;++i) {
//      if(i%50==0 && only) {
//        System.arraycopy(onlysip1, 0, byteips, i*4, 4);
//        System.arraycopy(onlydip1, 0, byteipd, i*4, 4);
//        System.arraycopy(onlysport, 0, bytesport, i*4, 2);
//        System.arraycopy(Bytes.toBytes("**"), 0, bytesport, i*4+2, 2);
//        System.arraycopy(onlydport, 0, bytedport, i*4, 2);
//        System.arraycopy(Bytes.toBytes("**"), 0, bytedport, i*4+2, 2);
//      } else if((i+1)%100==0 && only) {
//        System.arraycopy(onlysip2, 0, byteips, i*4, 4);
//        System.arraycopy(onlydip2, 0, byteipd, i*4, 4);
//        System.arraycopy(onlysport, 0, bytesport, i*4, 2);
//        System.arraycopy(Bytes.toBytes(1), 0, bytesport, i*4+2, 2);
//        System.arraycopy(onlydport, 0, bytedport, i*4, 2);
//        System.arraycopy(Bytes.toBytes("**"), 0, bytedport, i*4+2, 2);
//      } else {
//        System.arraycopy(tmpsip, 0, byteips, i*4, 4);
//        System.arraycopy(tmpdip, 0, byteipd, i*4, 4);
//        System.arraycopy(tmpsport, 0, bytesport, i*4, 2);
//        System.arraycopy(Bytes.toBytes("**"), 0, bytesport, i*4+2, 2);
//        System.arraycopy(tmpdport, 0, bytedport, i*4, 2);
//        System.arraycopy(Bytes.toBytes("**"), 0, bytedport, i*4+2, 2);
//      }
//      bytetu[i*4] = (byte)6;
//      System.arraycopy(Bytes.toBytes("***"), 0, bytetu, i*4+1, 3);
//    }
//    
//    indextmp = new byte[bigPackSize*4+tmp.length*bigPackSize];
//    byte[] ind = new byte[4];
//    ind = Bytes.toBytes(0);
//    for(int j=0;j<bigPackSize;++j) {
//      if(Bytes.toInt(ind)==0)
//        ind = Bytes.toBytes(bigPackSize*4+Bytes.toInt(ind));
//      else {
//    	ind = Bytes.toBytes(Bytes.toInt(ind)+tmp.length);
//	}
//      System.arraycopy(ind, 0, indextmp, j*4, ind.length);
//      System.arraycopy(tmp, 0, indextmp, Bytes.toInt(ind), tmp.length);
//    }
    metaPut.add(indexcf, a,bytetu);
    metaPut.add(indexcf, d,byteipd);
    metaPut.add(indexcf, dp,bytedport);
    metaPut.add(indexcf, s,byteips);
    metaPut.add(indexcf, sp,bytesport);
    metaPut.add(indexcf, z,byteokornot);
    metaPut.add(packetcf, z,indextmp);
    metatable.put(metaPut);
  }

  class PutThread extends Thread {
    public long donenum =0;
    String tableName = null;
    Vector<Integer> prefixs = null;
    byte[] rowkey = new byte[8];
    
    PutThread(String name,Vector<Integer> theprefixs) {
  	  this.tableName = name;
  	  prefixs = theprefixs;
    }
    
    public void run() {
      final int countPerThread = insertCount/threadN;
      try {
         HTableInterface metatable =null;
         metatable = pool.getTable(tableName);
         metatable.setAutoFlush(false);
         if(writeBuffer!=0) {
           metatable.setWriteBufferSize(writeBuffer);
         }
         for(int i=0;i<countPerThread;++i) {
      	 int pre = i%prefixs.size();
      	 int ppre = prefixs.get(pre);
      	 if(i%10==0){
      	   put2HbaseV2(metatable,wal,ppre,rowkey,true);
      	 } else {
      		put2HbaseV2(metatable,wal,ppre,rowkey,false);
      	 }
           donenum++;
         }
         metatable.flushCommits();
         metatable.close();
      } catch (IOException e) {
        e.printStackTrace();
      } 
    }
  }
   
  public void mutiput2(final String tableName,final int tt) throws InterruptedException{
	
    final PutThread putts[] = new PutThread[threadN>prefixCount?prefixCount:threadN];
    long insertstarttime = System.nanoTime();
    System.out.println("insert start time:  ");
    System.out.println(insertstarttime);
    int tn = threadN;
    int pn = prefixCount;
    int psn =prefixStartNum;
    Vector<Vector<Integer>> cd = new Vector<Vector<Integer>>();
    int num=0;
    for(int i=0;i<tn;++i){
	  cd.add(new Vector<Integer>());
    }
    for( int i=psn;i<(pn+psn);++i) {
	  cd.get(num).add(i);
	  num=(++num)%tn;
    }
    for (int i = 0; i < putts.length; i++) {
      putts[i] = new PutThread(tableName,cd.get(i));
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
//          if(currentnum<15000) {
//          	SimpleDateFormat matter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//              System.out.println(matter.format(new Date(System.currentTimeMillis())));
//          }
          currentnum=0;
        }
      }
    };
    th.start();
    
    for (int i = 0; i < putts.length; i++) {
      putts[i].join();
    }
    //Thread.sleep(Long.MAX_VALUE);
    th.stop();
    System.out.println("insert end time:  ");
    long insertendtime = System.nanoTime();
    System.out.println(insertendtime/1000/1000);
    String ops = Utils.getresult(insertCount,insertstarttime/1000/1000,insertendtime/1000/1000);
    System.out.println("put "+insertCount+", total time: "+(insertendtime - insertstarttime)/1000/1000
    		+" ms, average through: "+ ops+"/s");
  } 
   
  public static void main(String args[]) {
    //  args[0] config path,
    BigPacketPut bpp = new BigPacketPut();
    bpp.init(args[0]);
    try{
    	bpp.mutiput2(bpp.tableName,bpp.tt);
    } catch (InterruptedException e) {
    	e.printStackTrace();
    }
  }
}

