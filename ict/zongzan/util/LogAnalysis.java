package ict.zongzan.util;

import ict.zongzan.scheduler.Task;

import java.io.*;
import java.util.*;

/**
 * Created by Zongzan on 2016/11/27.
 */
public class LogAnalysis {

    private String waitTag = "[TASKWAIT]";
    private String startTag = "[TASKSTART]";
    private String endTag = "[TASKEND]";
    // lines which conatin task tag
    List<String> startList = new ArrayList<>();
    List<String> endList = new ArrayList<>();
    List<String> waitList = new ArrayList<>();
    // parse infomation from lines
    Map<String, String> taskInfo = new HashMap<>();
    Map<String, String> taskWait = new HashMap<>();
    Map<String, String> endInfo = new HashMap<>();
    private String workStart = "";
    private String workEnd = "";

    public static void main(String[] args) {

        if(args.length < 1) {
            System.out.println("Miss log file location.");
            return;
        }
        FileReader reader = null;
        LogAnalysis logLay = new LogAnalysis();
        //"C:\\Users\\Zongzan\\Desktop\\TASK\\11\\AppMaster.stdout"
        try {
            reader = new FileReader(args[0]);
            logLay.parser(reader);
            logLay.printInfo();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * 打印统计指标
     * 包括：总完成时间，平均job完成时间，总等待时间
     */
    void printInfo(){
        long totalRuntime = Long.parseLong(workEnd) - Long.parseLong(workStart);
        double avgTime= 0;
        double totalWaittime = 0;
        // get waitTime
        Set<String> keySet = taskInfo.keySet();
        for(String key : keySet){
            long starttime = Long.parseLong(taskInfo.get(key).split("\t")[2]);
            long waittime = Long.parseLong(taskWait.get(key));
            totalWaittime += starttime - waittime;
        }
        avgTime = (double)totalRuntime / (double)keySet.size();
        avgTime /= 1000.0;
        totalWaittime /= 1000.0;
        System.out.println("Job total runtime " + totalRuntime / 1000.0 + "s" +
                        "\nAverage runtime " + avgTime + "s" +
                        "\nJob total wait time " + totalWaittime + "s");
    }

    /**
     * 解析文件，找出tag中的信息
     * @param reader
     */
    void parser(FileReader reader) {
        BufferedReader br = new BufferedReader(reader);
        String line = null;
        try {
            while((line = br.readLine()) != null) {
                if(line.contains(startTag)){
                    startList.add(line);
                }
                else if (line.contains(endTag)) {
                    endList.add(line);
                }
                else if (line.contains(waitTag)) {
                    waitList.add(line);
                }
                else if (line.contains("[WORKSTART]")) {
                    workStart = line.split(",")[1];
                }
                else if (line.contains("WORKEND")) {
                    workEnd = line.split(",")[1];
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("startnum=" + startList.size() + " endnum=" + endList.size());

        for (String s : waitList) {
            String taskTag = "";
            String timestamp = "";
            String[] vals = s.split(",");
            for(int i = 1; i < vals.length; i++) {
                String[] val = vals[i].split("=");
                switch (val[0]) {
                    case "TASKTAG":
                        taskTag = val[1]; break;
                    case "TIMESTAMP":
                        timestamp = val[1]; break;
                }
            }
            taskWait.put(taskTag, timestamp);
        }


        //解析信息
        for(String s : startList){
            String taskTag = "";
            String timestamp = "";
            String containerId = "";
            String priority = "";
            String[] vals = s.split(",");
            for(int i = 1; i < vals.length; i++) {
                String[] val = vals[i].split("=");
                switch (val[0]) {
                    case "TASKTAG":
                        taskTag = val[1];   break;
                    case "TIMESTAMP":
                        timestamp = val[1]; break;
                    case "CONTAINERID":
                        containerId = val[1];   break;
                    case "PRIORITY":
                        priority = val[1];  break;
                }
            }
            taskInfo.put(taskTag, containerId + "\t" + priority  + "\t"+ timestamp);

        }

        for(String s : endList) {
            String taskTag = "";
            String timestamp = "";
            String[] vals = s.split(",");
            for(int i = 1; i < vals.length; i++) {
                String[] val = vals[i].split("=");
                switch (val[0]) {
                    case "TASKTAG":
                        taskTag = val[1];   break;
                    case "TIMESTAMP":
                        timestamp = val[1]; break;
                }
            }
            endInfo.put(taskTag, timestamp);

        }
        //输出
        System.out.println("num\ttaskTag\t\tcontainerId\t\t\tpriority\tstarttime\twaittime\tendtime");
        Set<String> keySet = taskInfo.keySet();
        Object[] keyList = keySet.toArray();
        ArrayList<String> sortList = new ArrayList<>();
        for(Object obj : keyList){
            sortList.add(obj.toString());
        }
        Collections.sort(sortList, new Comparator<Object>() {

            @Override
            public int compare(Object o1, Object o2) {
                String key1 = o1.toString().split("_")[0];
                String key2 = o2.toString().split("_")[0];
                if(Integer.parseInt(key1) > Integer.parseInt(key2))
                    return 1;
                else
                    return -1;

            }
        });

        int count = 0;
        for(String key : sortList) {
            System.out.println(count++ + "\t" + key + "-->" + taskInfo.get(key) + "\t" +
                    taskWait.get(key) + "\t" + endInfo.get(key));
        }
    }
}
