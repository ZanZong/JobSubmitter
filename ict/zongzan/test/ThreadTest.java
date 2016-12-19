package ict.zongzan.test;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

/**
 * Created by Zongzan on 2016/12/18.
 */
public class ThreadTest implements Runnable{

    int id = 0;
    int dura = 0;
    ThreadTest(int id, int dura){
        this.id = id;
        this.dura = dura;
    }
    @Override
    public void run() {
        try {
            Thread.sleep(dura);
            System.out.println("thread "+this.id+" end " + dura);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args){
        ThreadTest th1 = new ThreadTest(1, 1500);
        ThreadTest th2 = new ThreadTest(2, 2500);
        new Thread(th1).start();
        new Thread(th2).start();

    }
}
