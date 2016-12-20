package ict.zongzan.util;

import ict.zongzan.scheduler.Task;

import java.io.*;
import java.util.*;

/**
 * Created by Zongzan on 2016/11/27.
 */
public class LogAnalysis {

    private String startTag = "[TASKSTART]";
    private String endTag = "[TASKEND]";
    List<String> startList = new ArrayList<>();
    List<String> endList = new ArrayList<>();

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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(reader);
        String line = null;
        try {
            while((line = br.readLine()) != null) {
                if(line.contains(logLay.startTag)){
                    logLay.startList.add(line);
                }
                if (line.contains(logLay.endTag)) {
                    logLay.endList.add(line);
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
        System.out.println("startnum=" + logLay.startList.size() + " endnum=" + logLay.endList.size());

        Map<String, String> taskInfo = new HashMap<>();
        //解析信息
        for(String s : logLay.startList){
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
        Map<String, String> endInfo = new HashMap<>();
        for(String s : logLay.endList) {
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
        System.out.println("num\ttaskTag\t\tcontainerId\t\t\tpriority\tstarttime\tendtime");
        Set<String> keySet =  taskInfo.keySet();
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
            System.out.println(count++ + "\t" + key + "-->" + taskInfo.get(key) + "\t" + endInfo.get(key));
        }
    }
}
