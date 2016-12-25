package ict.zongzan.calculate;

/**
 * 该类是CloudArch中Original部分生成的汇编文件产生的
 * 可执行程序，直接提交到container中去执行
 * Created by Zongzan on 2016/12/18.
 */
public class CloudArchOriginal {


    public static long getloops(double csps, int cores) {
        double dura = csps / (double) cores;
        return (long)(dura * 440);
    }
}
