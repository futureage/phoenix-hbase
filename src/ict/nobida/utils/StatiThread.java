package ict.nobida.utils;

public class StatiThread extends Thread {
	long allnum = 0;
	long currentnum=0;
	long[] srcs = null;
	int statiTime = 0;
	public StatiThread(long[] rcs,int statt ){
		srcs = rcs;
		statiTime = statt;
	}
	public void run() {
		try {
			while(true) {
				Thread.sleep(statiTime*1000);
				for(int i =0; i< srcs.length;++i) {
					currentnum+=srcs[i];
					srcs[i]=0;
				}
				allnum +=currentnum;
					System.out.println(statiTime+" sec: "+allnum+" packets; "+
							Utils.getresult2(currentnum,statiTime)+" current packets/sec;");
				currentnum=0;
			}
		} catch (InterruptedException e) {
			//e.printStackTrace();
			return ;
		} finally {
		}
	}
}
