package ict.zongzan.calculate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Zongzan on 2016/11/10.
 */
public class CalcPI {

    int times;

    private static final Log LOG = LogFactory.getLog(CalcPI.class);

    public static double calculatePI(int times)
    {
        double pi = 0.0d;
        for(int i = 1; i <= times; i++)
        {
            pi += Math.pow(-1, (i + 1)) * 4 / (2 * i - 1);
        }
        return pi;
    }
    public static void main(String[] args){
        LOG.info("start calculate PI.\n");
        CalcPI cpi = new CalcPI();
        if(args.length == 0){
            LOG.info("use default times = 200000.\n");
            cpi.times = 200000;
        } else{
            cpi.times = Integer.parseInt(args[0]);
        }
        double res = calculatePI(cpi.times);

        System.out.print("The result of calculatePI is = " + res + "\n");
        LOG.info("Calculate end\n");
    }
}
