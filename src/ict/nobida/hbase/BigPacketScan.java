package ict.nobida.hbase;
import ict.filter.NetpacketFilter;
import ict.nobida.utils.Utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

//for big packet
public class BigPacketScan {
  public static final String HBASE_CONF ="hbase.config.path";
  public static final String COLUMN_FAMILYS="column.familys";
  public static final String PRE_SPLITS_COUNT = "pre.splits.count";
  public static final String TABLE_NAME ="table.name";
  public static final String FILTERS = "filters";
  public static final String SCAN_START_TIME= "scan.start.time";
  public static final String SCAN_END_TIME= "scan.end.time";
  public static final String SCAN_RESULT_FILEPATH= "scan.result.filepath";
  public static final String SCAN_LIMIT= "scan.limit.count";
  public static final String PACKET_NUM="packet.number";
  
  public static HTablePool pool = null;
  public  String tableName=null;
  FileInputStream fis;
  Properties pro = new Properties();
  String hbaseConf = null; 
  public static int preSplitsCount=0;
  public static Configuration config = new Configuration();
  String filters;
  String scanStart;
  String scanEnd;
  String scanReultsFilePath;
  int scanLimit=1000;
  int scanLimitDefault=1000;
  public List<NetpacketFilter> theFilters=null;
  public int packNum=0;  //indicate how many single packet in one big packet
  
  static public String[] cfs = {"b","d"}; //b mean basic ,d means data
  static public byte[] bcf = Bytes.toBytes("b");
  static public byte[] a = Bytes.toBytes("a");
  static public byte[] d = Bytes.toBytes("d");
  static public byte[] dp = Bytes.toBytes("dp");
  static public byte[] s = Bytes.toBytes("s");
  static public byte[] sp = Bytes.toBytes("sp");
  static public byte[] z = Bytes.toBytes("z");
  public byte[] pckap = {(byte) 0xd4,(byte) 0xc3 ,(byte) 0xb2,(byte) 0xa1 ,0x02,0x00 ,0x04,0x00 ,0x00,0x00,
		  0x00,0x00 ,0x00,0x00 ,0x00,0x00 ,(byte) 0xFF,(byte) 0xFF,
		  (byte) 0xFF,(byte) 0xFF,0x01,0x00 ,0x00,0x00};
  public void init(String path) {
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
      
    preSplitsCount = Integer.parseInt(pro.getProperty(PRE_SPLITS_COUNT));
    tableName = pro.getProperty(TABLE_NAME);
    String tmpf=pro.getProperty(FILTERS);
    if (tmpf!=null&&(!tmpf.equals("")))
      filters=tmpf;
    String tmpss=pro.getProperty(SCAN_START_TIME);
    if (tmpss!=null&&(!tmpss.equals("")))
      scanStart=tmpss;
    String tmpse=pro.getProperty(SCAN_END_TIME);
    if (tmpse!=null&&(!tmpse.equals("")))
      scanEnd=tmpse;
    String tmpsp=pro.getProperty(SCAN_RESULT_FILEPATH);
    if (tmpsp!=null&&(!tmpsp.equals("")))
      scanReultsFilePath=tmpsp;
    String tmpl=pro.getProperty(SCAN_LIMIT);
    if (tmpl!=null&&(!tmpl.equals("")))
      scanLimit = Integer.parseInt(tmpl);
//    String tmppn=pro.getProperty(PACKET_NUM);
//    if (tmppn!=null&&(!tmppn.equals("")))
//      packNum = Integer.parseInt(tmppn);
   // Properties prop = new Properties();
   // prop.setProperty("log4j.rootLogger", "ERROR");
   // PropertyConfigurator.configure(prop);
  }

  public List<Integer> docompare(final byte[] data,NetpacketFilter filter, List<Integer> indexs) {
	  int length = data.length;
	  int clen =0;
	  int interval =0;
	  List<Integer> fi= new ArrayList<Integer>();
	  byte[] qualifier = filter.getQualifier();
	  if(Bytes.compareTo(qualifier, s)==0|| Bytes.compareTo(qualifier, d)==0) {
	    clen= filter.getComparator().getValue().length;
	    //interval=4;
	  }
	  else if(Bytes.compareTo(qualifier, sp)==0 || Bytes.compareTo(qualifier, dp)==0){
	    clen = 2;
	    //interval=2;
	  }
	  else if(Bytes.compareTo(qualifier, a)==0){
	    clen = 1;
	    //interval=1;
	  }
	  interval=4;
	  byte[] tmp = new byte[clen];
	  if(indexs.size()==0) {
	  for (int i = 0; i < length; i += interval) {
	    for(int j=0; j< tmp.length;++j){
          tmp[j] = data[i+j];
		}
	    int compareResult = filter.getComparator().compareTo(tmp, 0, clen);
	      switch (filter.getOperator()) {
	      case LESS:
	        if (compareResult <= 0) {
	          continue;
	        } else {
	          fi.add(i/4);
	        }
	      case LESS_OR_EQUAL:
	        if (compareResult < 0) {
	          continue;
	        } else {
	        	fi.add(i/4);
	        }
	      case EQUAL:
	        if (compareResult != 0) {
	        	continue;
	        } else {
	        	fi.add(i/4);
	        }
	      case NOT_EQUAL:
	        if (compareResult == 0) {
	        	continue;
	        } else {
	        	fi.add(i/4);
	        }
	      case GREATER_OR_EQUAL:
	        if (compareResult > 0) {
	        	continue;
	        } else {
	        	fi.add(i/4);
	        }
	      case GREATER:
	        if (compareResult >= 0) {
	        	continue;
	        } else {
	        	fi.add(i/4);
	        }
	      default:
	        throw new RuntimeException("Unknown Compare op " + filter.getOperator().name());
	      }
	    }
	  } else {
        for(int i=0;i<indexs.size();++i) {
          int off = indexs.get(i);
          for(int j=0; j< tmp.length;++j){
            tmp[j] = data[j+off*interval];
          }
//          tmp[0] = data[off*4];
//          tmp[1] = data[1+ off*4];
//          tmp[2] = data[2+ off*4];
//          tmp[3] = data[3+ off*4];
            int compareResult = filter.getComparator().compareTo(tmp, 0, clen);
              switch (filter.getOperator()) {
              case LESS:
                if (compareResult <= 0) {
                  continue;
                } else {
                 fi.add(off);
                }
              case LESS_OR_EQUAL:
                if (compareResult < 0) {
                	continue;
                } else {
                  fi.add(off);
                }
              case EQUAL:
                if (compareResult != 0) {
                	continue;
                } else {
                  fi.add(off);
                }
              case NOT_EQUAL:
                if (compareResult == 0) {
                	continue;
                } else {
                  fi.add(off);
                }
              case GREATER_OR_EQUAL:
                if (compareResult > 0) {
                	continue;
                } else {
                  fi.add(off);
                }
              case GREATER:
                if (compareResult >= 0) {
                	continue;
                } else {
                  fi.add(off);
                }
              default:
                throw new RuntimeException("Unknown Compare op " + filter.getOperator().name());
              }
            }
	  }
	return fi;
  }
  
  public int toDiskByFilter(ScannerHeap sHeap,String filePath,long limit,List<NetpacketFilter> filters) {
    FileOutputStream fos = null;
    BufferedOutputStream bos = null;
    File file=null;
    int count =0;
    List<Integer> indexList = new ArrayList<Integer>();
    try {
    	if(filePath!=null) {
      file = new File(filePath);
      fos = new FileOutputStream(file,true);
      bos = new BufferedOutputStream(fos);
      bos.write(pckap, 0, 24);
    	}
      Result rr=null;
      byte[] tmpsip = null;
      byte[] tmpdip = null;
      byte[] tmpsport = null;
      byte[] tmpdport = null;
      byte[] tmptu = null;
      byte[] bigpack=null;
      byte[] singalpack=null;
      byte[] qualifier=null;
      SimpleDateFormat sdf = new SimpleDateFormat();
      String formatPattern = "yyyy-MM-dd-HH:mm:ss";
      sdf.applyPattern(formatPattern);
      
      while((count < limit)) {
        rr = sHeap.next();
        if(rr==null) {
          break;
        }
        //System.arraycopy(rr.getRow(),1,ts,0,rr.getRow().length-1);
        for (KeyValue kv : rr.raw()) {
          qualifier = kv.getQualifier();
          
          if(Bytes.compareTo(qualifier, d)==0) {
        	if(count==0) {
        	  packNum = kv.getValueLength()/4;
              tmpdip = new byte[kv.getValue().length];
        	}
            tmpdip = kv.getValue();
          } else if(Bytes.compareTo(qualifier, dp)==0) {
        	if(count==0){
              tmpdport = new byte[kv.getValue().length];
            }
            tmpdport = kv.getValue();
            //dp = ByteBuffer.wrap(kv.getValue()).getShort();
          } else if(Bytes.compareTo(qualifier, s)==0) {
        	if(count==0) {
              tmpsip = new byte[kv.getValue().length];
            }
            tmpsip = kv.getValue();
          } else if(Bytes.compareTo(qualifier, sp)==0) {
        	if(count==0){
              tmpsport = new byte[kv.getValue().length];
            }
            tmpsport = kv.getValue();
          } else if(Bytes.compareTo(qualifier, a)==0) {
        	if(count==0) {
              tmptu = new byte[kv.getValue().length];
            }
            tmptu = kv.getValue();
          } else {
        	bigpack = kv.getValue();
          }
        }
        if(filters!=null && filters.size()>0) {
        for(NetpacketFilter v:filters) {
          if(Bytes.compareTo(v.getQualifier(), s)==0)
            indexList= docompare(tmpsip, v, indexList);
        }
        for(NetpacketFilter v:filters) {
          if(Bytes.compareTo(v.getQualifier(), d)==0)
            indexList= docompare(tmpdip, v, indexList);
        }
        for(NetpacketFilter v:filters) {
          if(Bytes.compareTo(v.getQualifier(), sp)==0)
            indexList= docompare(tmpsport, v, indexList);
        }
        for(NetpacketFilter v:filters) {
          if(Bytes.compareTo(v.getQualifier(), dp)==0)
            indexList= docompare(tmpdport, v, indexList);
        }
        for(NetpacketFilter v:filters) {
          if(Bytes.compareTo(v.getQualifier(), a)==0)
            indexList= docompare(tmptu, v, indexList);
        }
        
        if(indexList.size()>0) {
          for(int i = 0;i<indexList.size()-1;++i) {
            // bigpack.length include the index's length, the offset in the index include index's length too.
            // so offset can use directly.
            int srcpos = Bytes.toInt(bigpack, indexList.get(i)*4, 4);
            int srclen = Bytes.toInt(bigpack, (indexList.get(i)+1)*4, 4)-srcpos;
            //System.out.println("srcpos is: "+srcpos+" srclen is: "+srclen);
            singalpack = new byte[srclen];
            System.arraycopy(bigpack, srcpos, singalpack, 0, srclen);
            ++count;
            if(filePath!=null){
            bos.write(singalpack, 0, singalpack.length);
            if(limit%100==0) {
              bos.flush();
            }
            }
          }
          int srcpos = Bytes.toInt(bigpack, indexList.get(indexList.size()-1)*4, 4);
          int srclen = bigpack.length-srcpos;
          singalpack = new byte[srclen];
          System.arraycopy(bigpack, srcpos, singalpack, 0, srclen);
          ++count;
          if(filePath!=null){
          bos.write(singalpack, 0, singalpack.length);
          bos.flush();
          }
          indexList.clear();
        }
      } else {
    	  count+=packNum;
    	  if(filePath!=null){
        bos.write(bigpack, packNum*4, bigpack.length-packNum*4);
        bos.flush();
    	  }
      }
      }
      if(filePath!=null){
      bos.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return count;
  }
  
  public int todiskbybytes(ScannerHeap sHeap,String filePath, int limit) {
    FileOutputStream fos = null;
    File file=null;
    BufferedOutputStream bos = null;
    int count =0;
    try {
      file = new File(filePath);
      fos = new FileOutputStream(file);
      bos = new BufferedOutputStream(fos);
      bos.write(pckap, 0, 24);
      Result rr=null;
      byte[] data=null;
      List<KeyValue> zrs = new ArrayList<KeyValue>();
      while((count < limit)) {
        rr = sHeap.next();
        if(rr==null) {
          break;
        }
        zrs = rr.getColumn(bcf, z);
        if(!zrs.isEmpty()) {
          for(KeyValue kv : zrs) {
            data = kv.getValue();
            bos.write(data, 0, data.length);
            ++count;
          }
        }
        zrs.clear();
        if(limit%100==0)
          bos.flush();
      }
      bos.flush();
      bos.close();
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
    public void close() {
      if (this.heap != null) {
        for(int i=0;i<rsArray.length;++i)
          if(rsArray[i]!=null) {
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
            // The heap is empty. Return the only possible scanner.
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
          // Compare the current scanner to the next scanner. We try to avoid
          // putting the current one back into the heap if possible.
          Result nextRes = nextEarliestPair.getFirst();
          if (nextRes == null || comparator.compare(rsPair, nextEarliestPair) < 0) {
            // We already have the scanner with the earliest KV, so return it.
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
    	int comparison; 
    	//System.out.println(Utils.toStringBinary(leftRow,0,leftRow.length) +" "+Utils.toStringBinary(rightRow,0,rightRow.length));
    	long re = Bytes.toLong(leftTs) - Bytes.toLong(rightTs);
    	if(re<0)
          comparison=-1;
    	else if(re==0)
          comparison=0;
    	else 
          comparison =1;
    	
        if (comparison != 0) {
          return comparison;
        } else {
          // Since both the keys are exactly the same, 
          System.out.println("the two scanner is equal..");
          return 0;
        }
      }
    }
  }
  
  public ResultScanner getScanners(HTableInterface hTable, byte[] startkey,
		  byte[] endkey,FilterList filterList, long limit) {
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
      if(limit<=scanLimitDefault){
        s.setCaching((int)limit);
      } else{
    	s.setCaching(scanLimitDefault);
      }
      //s.addColumn(cf, z);
      if(filterList!=null)
        s.setFilter(filterList);
      scanner = hTable.getScanner(s);
    }catch(IOException e) {
     e.printStackTrace();
    }
    return scanner;
  }

  public void scan(final String tableName, long scanStarttime,
		  long scanendtime, String filepath, String[] filters, long limit) throws InterruptedException, IOException {
    long starttime = System.currentTimeMillis();
    if(scanStarttime!=0 && scanendtime!=0 &&scanendtime<scanStarttime) {
	  System.out.println("scanStarttime shoudbe smaller than scanendtime");
	  return;
    }
    FilterList filterList = new FilterList();
    if(filters!=null) {
      theFilters = new ArrayList<NetpacketFilter>();
      for(String v:filters) {
        if(v!=null) {
          byte[] tmp=null;
          String [] ss=v.split(":");
          if(ss[0].equals(Bytes.toString(BigPacketScan.s))|| ss[0].equals(Bytes.toString(BigPacketScan.d))) {
            tmp = Utils.convertip(ss[1]);
          } else if(ss[0].equals(Bytes.toString(BigPacketScan.sp))|| ss[0].equals(Bytes.toString(BigPacketScan.dp))) {
            //ByteBuffer pBuffer= ByteBuffer.allocate(2);
            byte[] btmp = new byte[2];
            int inttmp= Integer.parseInt(ss[1]);
            btmp[1] = (byte)inttmp;
            inttmp >>>= 8;
            btmp[0] = (byte) inttmp;
            tmp = btmp;
          } else if(ss[0].equals(Bytes.toString(a))) {
            ByteBuffer tuBuffer= ByteBuffer.allocate(1);
            tuBuffer.put(Byte.parseByte(ss[1]));
            tmp = Bytes.toBytes(tuBuffer);
          } else {
            System.out.println("unknow filter: "+ss[0]+":"+ss[1]);
          }
          NetpacketFilter np = new NetpacketFilter(bcf,Bytes.toBytes(ss[0]),
          		CompareOp.EQUAL,tmp);
          filterList.addFilter(np);
          theFilters.add(np);
        }
      }  
    }
    long scanNum=0;
    List<ResultScanner> rsScanners=new ArrayList<ResultScanner>();
    HTableInterface hTable = BigPacketScan.pool.getTable(tableName);
    for (int i = 0; i < preSplitsCount; i++) {
      byte[] start = null;
      byte[] end = null;
      start = Utils.genRowkeyV2((byte)i,scanStarttime);
      end = Utils.genRowkeyV2((byte)i,scanendtime);
      
      ResultScanner rs = getScanners(hTable, start, end, filterList, limit);
      rsScanners.add(rs);
    }
    
   	try {
	  ScannerHeap sh = new ScannerHeap(rsScanners);
      scanNum = toDiskByFilter(sh,filepath,limit,theFilters);//todiskbybytes(sh,filepath,limit);
	} catch (IOException e) {
      e.printStackTrace();
    }
	long endtime = System.currentTimeMillis();
    String ops = Utils.getresult(scanNum,starttime,endtime);
    System.out.println("total get "+scanNum +".");// the total time is: "+(endtime - starttime)+
    		//"ms, average throughput: "+ ops+"/s .");
  } 

  String scanStartTime;
  String scanEndTime;
  String theFilter;
  String scanResultPath;
  String scanLimitCount;
  
  public static void main(String args[]) throws InterruptedException {
    BigPacketScan bps = new BigPacketScan();
    //args[0] config path,
    bps.init("putconf");
    
//    if(args.length<1) {
//      System.out.println("please input the Parameters: ");
//      System.out.println("st=2013-10-10-09:30:00  et=2013-10-12-10:00:00  filter={s:251.0.0.1} result=result count=1000");
//      return;
//    }
    for(int i=0;i<args.length;i++) {
      String[] tmparg = args[i].split("=");
      if(tmparg[0].equals("st")) {
        bps.scanStartTime = tmparg[1].substring(1,tmparg[1].length()-1);
      } else if(tmparg[0].equals("et")) {
        bps.scanEndTime = tmparg[1];
      }else if(tmparg[0].equals("filter")) {
        bps.theFilter = tmparg[1].substring(1,tmparg[1].length()-1);
      } else if(tmparg[0].equals("result")) {
        bps.scanResultPath = tmparg[1];
      } else if(tmparg[0].equals("count")) {
        bps.scanLimitCount = tmparg[1];
      }
    }
//    if(bps.scanLimitCount==null) {
//        System.out.println("please input the count: ");
//        System.out.println("count=1000");
//        return;
//    } 
    String[] theFilters = null;
    if(bps.theFilter!=null)
      theFilters = bps.theFilter.split(",");
    long start=0;
    long end =0;
    if(bps.scanStartTime==null||bps.scanEndTime==null) {
      System.out.println("please attention!!!");
      System.out.println("you should input the time range: yyyy-MM-dd-HH:mm:ss");
      //return ;
      end = Long.MAX_VALUE;
    } else {
      start = Utils.timeChange(bps.scanStartTime, null);
      end = Utils.timeChange(bps.scanEndTime, null);
    }
    if(start==-1 || end ==-1)
		return;
    File file=null;
    if(bps.scanResultPath!=null) {
    file = new File(bps.scanResultPath);
    if(file.exists()) {
      file.delete();
    }
    }
    long num=-1;
    if(bps.scanLimitCount!=null){
    	num = Long.parseLong(bps.scanLimitCount);
    }
    
    try {
		bps.scan(bps.tableName, start, end, 
				bps.scanResultPath,theFilters,num);
	} catch (Exception e) {
		e.printStackTrace();
	}
  }
}

