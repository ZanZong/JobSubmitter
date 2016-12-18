package ict.zongzan.calculate;

/**
 * 该类是CloudArch中Original部分生成的汇编文件产生的
 * 可执行程序，直接提交到container中去执行
 * Created by Zongzan on 2016/12/18.
 */
public class CloudArchOriginal {
    // 这里记录的是每个文件循环10000次所需的时间
    public static final String core1_new = "12.21" ;

    public static long getloops(double csps, int cores) {
        double dura = csps / (double) cores;
        return (long)(dura * 700);
    }
}
