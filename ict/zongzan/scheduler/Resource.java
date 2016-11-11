package ict.zongzan.scheduler;

/**
 * Created by Zongzan on 2016/11/9.
 */
public class Resource {
    // 使用cpu核数
    private int cpuCoresNum = 1;
    // 使用内存大小
    private int RAM = 100;
    // 磁盘大小
    private int localDiskSpace = 0;

    public Resource(int cpuCoresNum, int RAM, int localDiskSpace) {
        this.cpuCoresNum = cpuCoresNum;
        this.RAM = RAM;
        this.localDiskSpace = localDiskSpace;
    }

    public Resource(int cpuCoresNum, int RAM){
        this.cpuCoresNum = cpuCoresNum;
        this.RAM = RAM;
    }

    public int getCpuCoresNum() {
        return cpuCoresNum;
    }

    public void setCpuCoresNum(int cpuCoresNum) {
        this.cpuCoresNum = cpuCoresNum;
    }

    public int getRAM() {
        return RAM;
    }

    public void setRAM(int RAM) {
        this.RAM = RAM;
    }

    public int getLocalDiskSpace() {
        return localDiskSpace;
    }

    public void setLocalDiskSpace(int localDiskSpace) {
        this.localDiskSpace = localDiskSpace;
    }
}
