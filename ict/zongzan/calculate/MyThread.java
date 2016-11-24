package ict.zongzan.calculate;

/**
 * Created by Zongzan on 2016/11/24.
 */
class MyThread extends Thread{
    private String name;
    public MyThread(String name) {
        super();
        this.name = name;
        }
    public void run(){
        for(int i=0;i<10;i++){
            System.out.println("线程开始："+this.name+",i="+i);
            }
        }

    public static void main(String[] args) {
        MyThread mt1=new MyThread("线程a");
        MyThread mt2=new MyThread("线程b");
        mt1.run();
        mt2.run();
        }
    }
