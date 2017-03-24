package ict.calculate;

/**
 * TaskExecutor是一个抽象类，它包含提交的作业中，
 * 所必须的一些成员变量和方法。
 * 由该Application执行的task程序实现该接口
 * Created by Zongzan on 2016/11/21.
 */
public abstract class TaskExecutor {

    public double cpuUsage;

    public int cores;

    private double duration = 0;

    public TaskExecutor(double cpuUsage, int cores) {
        this.cpuUsage = cpuUsage;
        this.cores = cores;
    }

    /**
     * 获得需要执行的时长
     * @return
     */
    public double getDuration(){
        if (cores == 0){
            return 0;
        }
        else {
            duration = (cpuUsage / cores);
        }
        return duration;
    }

    /**
     * 由子类实现不同的计算过程
     * @param loops
     */
    public abstract void calculate(int loops);

}
