package ict.nobida.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.generated.regionserver.regionserver_jsp;
import org.apache.hadoop.hbase.util.Bytes;

public class Utils {

  public static byte[] genRowkeyV2(byte k, long ts) {
    ByteBuffer tmp = ByteBuffer.allocate(9);
    tmp.put(k);
    return tmp.putLong(ts).array();
  }
  public static byte[] genRowkeyV3(byte k) {
    ByteBuffer tmp = ByteBuffer.allocate(9);
    tmp.put(k);
    for(int i=1;i<9;++i)
    	tmp.put((byte)0xFF);
    return tmp.array();
  }
  
  public static byte[] genRowkeyforTopK(byte k, byte[] sk,long ts,byte[]dk) {
	    ByteBuffer tmp = ByteBuffer.allocate(17);
	    tmp.put(k);
	    tmp.put(sk);
	    tmp.putLong(ts);
	    tmp.put(dk);
	    return tmp.array();
  }
  
  public static byte[] genRowkeyforidx(byte k, int v, byte[] sk) {
	    ByteBuffer tmp = ByteBuffer.allocate(14);
	    byte []vb = Bytes.toBytes(v);
	    tmp.put(k);
	    //tmp.put(vb, 1, 3);
	    tmp.putInt(v);
	    tmp.put(sk);
	    return tmp.array();
}
  public static byte[] genRowkeyforidxscan(byte k, int v, byte p,long sk) {
	    ByteBuffer tmp = ByteBuffer.allocate(14);
	    byte []vb = Bytes.toBytes(v);
	    tmp.put(k);
	    //tmp.put(vb, 1, 3);
	    tmp.putInt(v);
	    tmp.put(p);
	    tmp.putLong(sk);
	    return tmp.array();
}
  public static int toInt(byte[] bytes, int offset, final int length) throws Exception {
	    if (offset + length > bytes.length) {
	      throw new Exception();
	    }
	    int n = 0;
	    for(int i = offset; i < (offset + length); i++) {
	      n <<= 8;
	      n ^= bytes[i] & 0xFF;
	    }
	    return n;
  }
 
  public static byte[][] getSplitV2(int k) {
	byte[][] ret = new byte[k - 1][];
	byte[]tmp = new byte[9];
	int start = 0;
	Arrays.fill(tmp, 1, 8, (byte) 0);
	for (int i = 1; i < k; i++) {
//	  ByteBuffer tmp = ByteBuffer.allocate(9);
//	  tmp.put((byte)(i));
//    ret[i - 1]=tmp.putLong((long)0).array();
	  tmp[0] = (byte)(i+start);
	  ret[i-1] = Arrays.copyOf(tmp, tmp.length);
	}
	return ret;
  }
  
  public static byte[][] getSplitidx(int k) {
	byte[][] ret = new byte[k - 1][];
	byte[]tmp = new byte[14];
	Arrays.fill(tmp, 1, 13, (byte) 0);
	for (int i = 1; i < k; i++) {
	  tmp[0] = (byte)(i);
	  ret[i-1] = Arrays.copyOf(tmp, tmp.length);
	}
	return ret;
  }
  
  public static byte[] convertip(String ipstr) {
    String[]tmp = ipstr.split("\\.");
    byte[] ret = new byte[tmp.length];
    if(tmp[0].equals("*")) {
      System.out.println("the ip is error.");
      return null;
    }
    for (byte i = 0; i < tmp.length; i++) {
      if(tmp[i].equals("*")) {
        byte[] preret = new byte[i];
        for(int j=0;j<i;j++){
          preret[j] = ret[j];
        }
        return preret;
      }
      ret[i]=(byte)(Integer.parseInt(tmp[i]) & 0xFF);
    }
    return ret;
  }
  public static String back2ip(byte[] bs ){
  //byte[] ss = convertip("1.0.0.1");
    String aa="";
    for(int i=0;i<bs.length-1;++i){
      aa+= String.valueOf( (int)(bs[i] & 0xFF))+".";
    }
    aa+= String.valueOf( (int)(bs[bs.length-1] & 0xFF));
    return aa;
  }
  
  public static String base = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";   
  public static String getRandomString(int length) {   
    Random random = new Random();   
    StringBuffer sb = new StringBuffer();   
    for (int i = 0; i < length; i++) {   
      int number = random.nextInt(base.length());   
      sb.append(base.charAt(number));
    }   
    return sb.toString();
  }
  
/**
 * Write a printable representation of a byte array. Non-printable
 * characters are hex escaped in the format \\x%02X, eg:
 * \x00 \x05 etc
 *
 * @param b array to write out
 * @param off offset to start at
 * @param len length to write
 * @return string output
 */
  public static String toStringBinary(final byte [] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    for (int i = off; i < off + len ; ++i ) {
      int ch = b[i] & 0xFF;
      if ( (ch >= '0' && ch <= '9')
          || (ch >= 'A' && ch <= 'Z')
          || (ch >= 'a' && ch <= 'z')
          || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0 ) {
          result.append((char)ch);
      } else {
        result.append(String.format("\\x%02X", ch));
      }
    }
    return result.toString();
  }

  public static String getresult(long count, long starttime, long endtime) {
    Format format=new DecimalFormat("0.00");
    double time = Double.parseDouble(format.format((double)(endtime-starttime)/1000));
    return format.format((double)count/time);
  }
  
  public static String getresult2(long count,long range) {
    Format format=new DecimalFormat("0.00");
    double time = Double.parseDouble(format.format((double)range));
    return format.format((double)count/time);
  }

  public static long timeChange(String Stime,String formatPattern) {
    long ltime=0;
    Date dt = null;
    if(Stime ==null) {
      Stime = "2013-10-10 09:30:00";
    }
    SimpleDateFormat sdf = new SimpleDateFormat();
    if(formatPattern==null||formatPattern.equals("")){
      formatPattern = "yyyy-MM-dd HH:mm:ss";
    }
    sdf.applyPattern(formatPattern);
    try {
	  dt= sdf.parse(Stime);
	  ltime = dt.getTime();
	} catch (ParseException e) {
	  System.out.println("wrong date format: "+Stime);
	  return -1;
	}
    //System.out.println(ltime*1000000);
    return ltime*1000000;
  }
  public static long timeChange2(String Stime, String formatPattern) {
	    long ltime = 0;
	    Date dt = null;
	    if(Stime ==null){
	      Stime = "2013-10-10 09:30:00";
	    }
	    SimpleDateFormat sdf = new SimpleDateFormat();
	    if(formatPattern==null||formatPattern.equals("")){
	      formatPattern = "yyyy-MM-dd HH:mm:ss";
	    }
	    sdf.applyPattern(formatPattern);
	    try {
		  dt= sdf.parse(Stime);
		  ltime = dt.getTime();
		} catch (ParseException e) {
		  System.out.println("wrong date format: "+Stime);
		  return -1;
		}
	    return ltime;
	  }
  public static long timeChange2sec(String Stime,String formatPattern){
    long ltime=0;
    Date dt = null;
    SimpleDateFormat sdf = new SimpleDateFormat();
    if(formatPattern==null||formatPattern.equals("")){
      formatPattern = "yyyy-MM-dd HH:mm:ss";
    }
    sdf.applyPattern(formatPattern);
    try {
	  dt= sdf.parse(Stime);
	  ltime = dt.getTime();
	} catch (ParseException e) {
	  e.printStackTrace();
	}
    //System.out.println(ltime*1000000);
    return ltime*1000000;
  }
  
  public static String long2time(byte[] rowkey) {
		byte[] rk =  new byte[8];
		System.arraycopy(rowkey, 1, rk, 0, 8);
		//System.out.println(Utils.toStringBinary(rk, 0, rk.length));
		long tt = Bytes.toLong(rk);
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//先得到毫秒数，再转为java.util.Date类型
		java.util.Date dt = new Date(tt / 1000000);  
		String sDateTime = sdf.format(dt);
		//System.out.println(sDateTime);
		return sDateTime;
  }
  public void thred () {
	  new Thread(new Runnable() {
			@Override
			public void run() {
				FileOutputStream fos = null;
			    BufferedOutputStream bos = null;
			    File file = new File("res");
			    try {
			      fos = new FileOutputStream(file,false);
			      bos = new BufferedOutputStream(fos);
			      bos.write(12);
				  bos.close();
				  fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
	    System.out.println("ok");
  }
  public static  void dd (String d,String d1){
	  if(StringUtils.isBlank(d))
		  System.out.println("hahah");
	  System.out.println("dddd");
	  System.out.println(d1);
  }
  public static void filetest() throws IOException{
	  File cFile = new File("/home/hadoop/1");
		if (cFile.exists()) {
			FileReader fReader = new FileReader(cFile);
			System.out.println(fReader.read());
			//FileInputStream fos = new FileInputStream(cFile);
		}
  }
  public int cc = 10;
  
  public static List<List<String>> getIP4phoenix(int dist) {
	  List<List<String>> data = new ArrayList<List<String>>(dist);
	  String sip = "10.0.0.";
	  String dip = "192.168.0.";
	  String sip1 = null;
	  String dip1 = null;
	  String sport = null;
	  String dport = null;
	  
	  for(int i = 1; i<= dist; ++i) {
		  List<String> one = new ArrayList<String>(4);
		  sip1 = sip + i;
		  dip1 = dip + i;
		  sport = String.valueOf(i);
		  dport = String.valueOf(i+dist);
		  one.add(dip1);
		  one.add(dport);
		  one.add(sip1);
		  one.add(sport);
		  data.add(one);
	  }
	  return data;
  }
  public static List<List<String>> getIP4phoenixmutable(int dist, Random rand) {
	  int size = dist*dist;
	  List<List<String>> data = new ArrayList<List<String>>(size);
	  String sip = "10.0.0.";
	  String dip = "192.168.0.";
	  String sip1 = null;
	  String dip1 = null;
	  String sport = null;
	  String dport = null;
	  try {
	  for(int i = 0; i< size; ++i) {
		  List<String> one = new ArrayList<String>(4);
		  int rsip = rand.nextInt(dist)+1;
		  Thread.sleep(1);
		  int rdip = rand.nextInt(dist)+1;
		  
		  dip1 = dip + rdip;
		  dport = String.valueOf(dist+rdip);
		  sip1 = sip + rsip;
		  sport = String.valueOf(rsip);

		  one.add(dip1);
		  one.add(dport);
		  one.add(sip1);
		  one.add(sport);
		 // System.out.println(dip1 + " "+ dport + " "+ sip1 + " "+ sport);
		  data.add(one);
	  }
	  } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  return data;
  }
  public static void main(String[] args) throws IOException, InterruptedException {
    //timeChange("2013-09-14-14:32:00", null);
    //convertip("127.0.0.1");
	  
	  
	  getIP4phoenix(10);
	  
    ByteBuffer bb = ByteBuffer.allocate(1);
    bb.put((byte)6);
    byte[]a =  Bytes.toBytes(bb);
    //System.out.println(bb.wrap(a).getChar());
    System.out.println(a.length);
    System.out.println(toStringBinary(a, 0, 1));
    byte[]b = Bytes.toBytes((byte)1);
    
    String dd = "";
    List<String> dsf = new ArrayList<String>();
    dsf.add(dd);
    System.out.println(dsf.get(0));
    dd(dd,"ddsdf");
    
    
    String sda = new String("ad="+dd);
    String[] df = sda.split("=");
    System.out.println(df.length);
    for (int i = 0; i < df.length; i++) {
	System.out.println(df[i]);
	}
    
    Map<String, Integer> sipSitat =new HashMap<String, Integer>();
    sipSitat.put("A", 98);
    sipSitat.put("B", 50);
    sipSitat.put("C", 76);
    sipSitat.put("D", 23);
    sipSitat.put("E", 85);
//    List<Map.Entry<String, Integer>> siplist = new ArrayList<Map.Entry<String, Integer>>(sipSitat.entrySet());
//  	Collections.sort(siplist, new Comparator<Map.Entry<String, Integer>>() {   
//  	    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {      
//  	        return (int)(o2.getValue() - o1.getValue()); 
//  	    }
//  	});
  	
  	for(Iterator<Map.Entry<String, Integer>> iterator = sipSitat.entrySet( ).iterator( );iterator.hasNext();){
  	  System.out.println(iterator.next().getKey());
  	}
  }
}
