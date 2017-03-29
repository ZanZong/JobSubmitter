package ict.util;

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
    Map<String, String> startInfo = new HashMap<>();
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
     * print
     * include totalruntime, average run time, total wait time
     */
    void printInfo(){
        // get total runtime
        long totalRuntime = getMaxtime(endInfo) - getMintime(startInfo);
        double avgTime= 0;
        double totalWaittime = 0;
        // get waitTime
        Set<String> keySet = startInfo.keySet();
        for(String key : keySet){
            long starttime = Long.parseLong(startInfo.get(key).split("\t")[2]);
            long waittime = Long.parseLong(taskWait.get(key));
            totalWaittime += starttime - waittime;
        }
        long pureRuntime = 0;
        for(String key : keySet) {
            long starttime = Long.parseLong(startInfo.get(key).split("\t")[2]);
            long endtime = Long.parseLong(endInfo.get(key));
            pureRuntime += endtime - starttime;
        }
        avgTime = (totalRuntime + pureRuntime) / (double)keySet.size();
        avgTime /= 1000.0;
        totalWaittime /= 1000.0;
        System.out.println("Job total runtime " + totalRuntime / 1000.0 + "s" +
                        "\nAverage runtime " + avgTime + "s" +
                        "\nJob total wait time " + totalWaittime + "s");
    }

    /**
     * log analysis
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
                /*else if (line.contains("[WORKSTART]")) {
                    workStart = line.split(",")[1];
                }
                else if (line.contains("WORKEND")) {
                    workEnd = line.split(",")[1];
                }*/
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
        //System.out.println("startnum=" + startList.size() + " endnum=" + endList.size());

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
            startInfo.put(taskTag, containerId + "\t" + priority  + "\t"+ timestamp);
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
        //out
        System.out.println("num\ttaskTag\t\tcontainerId\t\t\tpriority\tstartrun\ttaskstartwait\tend");
        Set<String> keySet = startInfo.keySet();
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
            System.out.println(count++ + "\t" + key + "-->" + startInfo.get(key) + "\t" +
                    taskWait.get(key) + "\t" + endInfo.get(key));
        }
    }

    public long getMaxtime(Map<String, String> map) {
        long time = 0;
        for(String s : map.keySet()){
            long t = Long.parseLong(map.get(s));
            if(t > time)
                time = t;
        }
        return time;
    }
    public long getMintime(Map<String, String> map) {
        Long time = new Long("2903593275134");
        for(String s : map.keySet()){
            long t = Long.parseLong(map.get(s).split("\t")[2]);
            if(t < time)
                time = t;
        }
        return time;
    }
}
