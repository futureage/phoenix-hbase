package ict.nobida.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.antlr.grammar.v3.ANTLRParser.finallyClause_return;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

public class Init {
	public static HTablePool pool = null;
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
	public static final String CACHE_COUNT="cache.count";
	public static final String SCAN_COUNT="scan.count";
	public static final String INDEX_COUNT="index.count";
	public static final String CURRENT_INDEX_NUM="current.index.num";
	public static final String ONLY_COUNT = "only.count";

	FileInputStream fis;
	Properties pro = new Properties();
	String hbaseConf = null; 
	public static int threadN=0;
	public static int preSplitsCount=0;
	public static Configuration config = new Configuration();//HBaseConfiguration.create();//获得Hbase配置参数
	static int startNum=0;
	public static long insertCount = 500;
	public static long getCount = 0;
	public static int getRange=0;
	public String getField;
	public String cfstring;
	public int tt =10;
	public boolean wal=true;
	public int datasize=50;
	public int prefixCount=0;
	public int prefixStartNum=0;
	public String filters;
	public String scanStart;
	public String scanEnd;
	public String scanReultsFilePath;
	public long scanLimit=0;
	public int cacheCount = 1000;
	boolean resultSort=true;
	public int bigPackSize=1;
	public int indexCount = 0;
	public int curridxnum=0;
	public boolean onlyCount=true;
	public static int dist = 30;
	public String zkAddr = null;
	
	public String idxt1 = null;
	public String idxt2 = null;
	public String idxt3 = null;
	public String idxt4 = null;

	public static AtomicLong counter= null;
	public Random rand = null;

	static public String[] cfs = {"b","d"}; //b mean basic ,d means data
	static public String[] idxcf = {"b"};
	static public byte[] indexcf = Bytes.toBytes("b");
	static public byte[] packetcf = Bytes.toBytes("d");
	static public byte[] a = Bytes.toBytes("a");
	static public byte[] d = Bytes.toBytes("d");
	static public byte[] dp = Bytes.toBytes("dp");
	static public byte[] s = Bytes.toBytes("s");
	static public byte[] sp = Bytes.toBytes("sp");
	static public byte[] z = Bytes.toBytes("z");
	public static byte[] tmp;
	public long[] rtCount = null;

	public void init(String path) {
		long seed = System.nanoTime();
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

		threadN = Integer.parseInt(pro.getProperty(THREAD_NUM));
		startNum = Integer.parseInt(pro.getProperty(START_NUM));
		counter = new AtomicLong(startNum);
		insertCount = Long.parseLong(pro.getProperty(INSERT_COUNT));
		getRange = (int)insertCount;
		preSplitsCount = Integer.parseInt(pro.getProperty(PRE_SPLITS_COUNT));
		tableName = pro.getProperty(TABLE_NAME);
		
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
		String tmpse=pro.getProperty(SCAN_END_TIME);
		if (tmpse!=null&&(!tmpse.equals("")))
			scanEnd=tmpse.substring(1,tmpse.length()-1);
		String tmpbps=pro.getProperty(BIG_PACKET_SIZE);
		if (tmpbps!=null&&(!tmpbps.equals("")))
			bigPackSize = Integer.parseInt(tmpbps);
		
		String tmpsc=pro.getProperty(SCAN_COUNT);
		if (tmpsc!=null&&(!tmpsc.equals(""))){
			scanLimit = Long.parseLong(tmpsc);
			getCount = scanLimit;
		}
		
		
		String tmpscc=pro.getProperty(CACHE_COUNT);
		if (tmpscc!=null&&(!tmpscc.equals("")))
			cacheCount = Integer.parseInt(tmpscc);
		
		String tmpidc=pro.getProperty(INDEX_COUNT);
		if (tmpidc!=null&&(!tmpidc.equals("")))
			indexCount = Integer.parseInt(tmpidc);
		
		String tmpcidn=pro.getProperty(CURRENT_INDEX_NUM);
		if (tmpcidn!=null&&(!tmpcidn.equals("")))
			curridxnum = Integer.parseInt(tmpcidn);
		String tmpoc=pro.getProperty(ONLY_COUNT);
		if (tmpoc!=null&&(!tmpoc.equals("")))
			onlyCount = Boolean.parseBoolean(tmpoc);
		
		String tmpdist=pro.getProperty("distinct.count");
		if (tmpdist!=null&&(!tmpdist.equals("")))
			dist = Integer.parseInt(tmpdist);
		
		String tmpzk=pro.getProperty("zk.addr");
		if (tmpzk!=null&&(!tmpzk.equals("")))
			zkAddr = tmpzk;
		int poolSize = 0;
		if(indexCount!=0) {
			if(indexCount == 1){
				idxt1 = "idxt1";
				poolSize = preSplitsCount*2;
			}
			if(indexCount == 2){
				idxt1 = "idxt1";
				idxt2 = "idxt2";
				poolSize = preSplitsCount*3;
			}
			if(indexCount == 3) {
				idxt1 = "idxt1";
				idxt2 = "idxt2";
				idxt3 = "idxt3";
				poolSize = preSplitsCount*4;
			}
			if(indexCount == 4) {
				idxt1 = "idxt1";
				idxt2 = "idxt2";
				idxt3 = "idxt3";
				idxt4 = "idxt4";
				poolSize = preSplitsCount*5;
			}
		}
		
		tmp = Bytes.toBytes(Utils.getRandomString(datasize));
		int rtlen = preSplitsCount > threadN ? preSplitsCount : threadN;
		poolSize = poolSize > threadN ? poolSize :threadN;
		pool = new HTablePool(config, poolSize);
		rtCount = new long[rtlen];
		for(int i=0;i< rtlen;++i) {
			rtCount[i]=0;
		}
		
		Properties prop = new Properties();
		try {
			fis.close();
			fis = new FileInputStream("log4j.properties");
			prop.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		PropertyConfigurator.configure(prop);
	}
}
