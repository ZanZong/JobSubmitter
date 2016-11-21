package ict.zongzan.calculate;

/**
 * java程序对cpu资源的使用
 * 使用JavaCalcProgram对象，根据参数
 * 消耗相应的计算资源
 * Created by Zongzan on 2016/11/21.
 */
public class JavaCalcProgram extends TaskExecutor{

    public JavaCalcProgram(double cpuUsage, int cores){
        super(cpuUsage, cores);
    }

    public static void main(String[] args) {
        if (args.length != 2){
            System.out.println("Miss argument");
           return;
        }
        // core-s/s
        double core_sps =  Double.parseDouble(args[0]);
        // cores
        int cores = Integer.parseInt(args[1]);
        JavaCalcProgram jcp = new JavaCalcProgram(core_sps, cores);
        // 获得使用cores个核，所使用的时间
        double duration = jcp.getDuration();

        int loops = (int)(100000 * (duration / 0.013));
        long start = System.currentTimeMillis();
        jcp.calculate(loops);
        long end = System.currentTimeMillis();
        System.out.println(loops + " loops, duration:" + (end - start) + "ms"
                + ", CpuUsage:" + core_sps + "core-s/s.");
    }

    /**
     * 经测试，10,0000 loops耗时0.013s
     * @param
     */
    @Override
    public void calculate(int loops) {
        Long num = new Long("370481199403120310");
        for(int i = 0; i < loops; i++){
            int tmp = (int) (num % 86);
            num--;
        }
    }
}
