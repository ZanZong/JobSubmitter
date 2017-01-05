package ict.zongzan.util;

import org.apache.avro.generic.GenericData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * 通过LogAnalys解析得到的job的开始结束时间的各个指标
 * 再通过ResultStat分析、对比指标
 * 通过参数控制，选择较好的指标结果
 * Created by Zongzan on 2017/1/4.
 */
public class ResultStat {

    // 控制显示的个数，按指标最优排序
    int num;
    // all submit log id
    Set<String> submitId = new HashSet<>();
    // map submit-log id to total runtime
    Map<Double, String> totalRuntime = new HashMap<>();
    // map submit-log id to average runtime
    Map<Double, String> avgRuntime = new HashMap<>();
    // map submit-log id to total wait time
    Map<Double, String> totalWaittime = new HashMap<>();
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Miss log file location or the number of result to show\n" +
                    "ResultStatistic <file-location> <num>");
            return;
        }
        FileReader reader = null;
        ResultStat resultStat = new ResultStat();
        resultStat.num = Integer.parseInt(args[1]);
        try {
            // C:\Users\Zongzan\Desktop\TASK\16\tmp
            reader = new FileReader(args[0]);
            resultStat.statistics(reader, resultStat.num);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    void statistics(FileReader reader, int num) {
        BufferedReader br = new BufferedReader(reader);
        String line = null;
        try {
            String submit = "";
            while ((line = br.readLine()) != null) {
                // skip to has higher retrieval efficiency
                if(line.charAt(0) >= 48 && line.charAt(0) <= 57)
                    continue;
                if(line.contains("submit")) {
                    int len = line.split("log")[0].length() - 1;
                    String s = line.split("log")[0].substring(0, len);
                    submitId.add(s);
                    submit = s;
                }
                else if (line.contains("total runtime")) {
                    String[] tmp = line.split(" ");
                    totalRuntime.put(Double.parseDouble(tmp[tmp.length - 1].split("s")[0]), submit);
                }
                else if (line.contains("Average runtime")) {
                    String[] tmp = line.split(" ");
                    avgRuntime.put(Double.parseDouble(tmp[tmp.length - 1].split("s")[0]), submit);
                }
                else if (line.contains("wait time")) {
                    String[] tmp = line.split(" ");
                    totalWaittime.put(Double.parseDouble(tmp[tmp.length - 1].split("s")[0]), submit);
                }
            }
        }catch(IOException e){
            e.printStackTrace();
        }finally{
            try {
                br.close();
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        List<Double> totalRuntimeList = sortKey(totalRuntime);
        List<Double> avgRuntimeList = sortKey(avgRuntime);
        List<Double> totalWaittimeList = sortKey(totalWaittime);
        System.out.println("top " + num + " of total runtime:");
        for(int i = 0; i < num; i++) {
            Double t = totalRuntimeList.get(i);
            System.out.println((i + 1) + "\t" + totalRuntime.get(t)
                    + "\t" + t);
        }
        System.out.println("top " + num + " of average time:");
        for(int i = 0; i < num; i++) {
            System.out.println((i + 1) + "\t" + avgRuntime.get(avgRuntimeList.get(i))
                    + "\t" + avgRuntimeList.get(i));
        }
        System.out.println("top " + num + " of total wait time:");
        for(int i = 0; i < num; i++) {
            System.out.println((i + 1) + "\t" + totalWaittime.get(totalWaittimeList.get(i))
                    + "\t" +totalWaittimeList.get(i));
        }
    }

    // 对map的key升序排序，返回key的有序列表，
    public List<Double> sortKey(Map map) {
        Object[] keyList = map.keySet().toArray();
        List<Double> list = new ArrayList();
        for(Object o : keyList) {
            list.add(Double.parseDouble(o.toString()));
        }
        Collections.sort(list);
        return list;
    }
}
