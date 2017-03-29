package ict.calculate;

/**
 * 该类用来计算在使用c程序作为task时，
 * 根据给定的计算资源，应该循环多少次
 * 根据测试，单线程，1 second --> 7600000 loops
 * loops最大值为2000000000，超过会溢出
 * Created by Zongzan on 2016/11/22.
 */
public class IbsCProgram {

    public static String getLoops(double csps, double cores) {
        // 单线程，先不考虑cores
        return String.valueOf((long)(csps * 7600000));
    }

}
