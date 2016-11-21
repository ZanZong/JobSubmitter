package ict.zongzan.util;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import ict.zongzan.scheduler.Job;
import ict.zongzan.scheduler.Resource;
import ict.zongzan.scheduler.Task;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import sun.reflect.generics.reflectiveObjects.LazyReflectiveObjectGenerator;

/**
 * 从XML文件中读取job数据,转换成相应的model
 * Created by Zongzan on 2016/11/18.
 */

public class JobLoader {

    private static final Log LOG = LogFactory.getLog(JobLoader.class);

    private String xmlPath = "";


    public JobLoader(String xmlUrl) {
        this.xmlPath = xmlUrl;
    }

    public Job getJobFromXML() {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        DocumentBuilder builder = null;

        Document document = null;
        try {
            builder = factory.newDocumentBuilder();
            document = builder.parse(new InputSource(new FileInputStream(xmlPath)));
            System.out.println("read xml file.");
        } catch (ParserConfigurationException e) {
            LOG.error("Load job from XML error.");
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            LOG.error("Load job from XML error.");
            e.printStackTrace();
        } catch (IOException e) {
            LOG.error("Load job from XML error.");
            e.printStackTrace();
        } catch (SAXException e) {
            LOG.error("Load job from XML error.");
            e.printStackTrace();
        }
        return getModel(document);
    }

    private Job getModel(Document doc) {
        Job job = new Job();
        List<Task> taskList = new ArrayList<Task>();
        int priority = 0;
        Element rootElement = doc.getDocumentElement();
        NodeList nodeList = rootElement.getChildNodes();
        int len = nodeList.getLength();
        for(int i = 0; i < len; i++){
            Node node = nodeList.item(i);
            // 这个node不一定是有值的，DOM把tasks标签也会当做一个node
            // 所以要先判断是不是Element
            if(node instanceof Element){
            //为job属性
                String content = node.getLastChild().getTextContent().trim();
                switch (node.getNodeName()) {
                    case "jobId":
                        job.setJobId(content);                  break;
                    case "jobName":
                        job.setJobName(content);                break;
                    case "description":
                        job.setDescription(content);            break;
                    case "priority":
                        priority = Integer.parseInt(content);   break;
                    case "tasks": {
                        // getChild
                        // 这样得到的一定是Element
                        NodeList tasks = ((Element) node).getElementsByTagName("task");
                        //System.out.println(tasks.getLength());

                        for (int j = 0; j < tasks.getLength(); j++){
                            Task t = new Task();
                            int cores = 0;
                            int ram = 0;
                            double csps = 0;
                            Node task = tasks.item(j);
                            NodeList taskItems = task.getChildNodes();
                            for(int k = 0; k < taskItems.getLength(); k++){
                                // task
                                Node taskItem = taskItems.item(k);
                                if(taskItem instanceof Element){
                                    //System.out.println("nodename:"+taskItem.getNodeName());
                                    String taskCon = taskItem.getFirstChild().getNodeValue().trim();
                                    //System.out.println("value:"+taskCon);
                                    switch (taskItem.getNodeName()){
                                        case "taskId":
                                            t.setTaskId(taskCon);                           break;
                                        case "execSequence":
                                            t.setExecSequence(Integer.parseInt(taskCon));   break;
                                        case "jarPath":
                                            t.setJarPath(taskCon);                          break;
                                        case "resourceAlloc":{
                                            // resource
                                            NodeList resItems = taskItem.getChildNodes();
                                            for(int m = 0; m < resItems.getLength(); m++){
                                                Node resItem = resItems.item(m);
                                                if(resItem instanceof Element){
                                                    String resAlloc = resItem.getFirstChild().getNodeValue().trim();
                                                    switch (resItem.getNodeName()) {
                                                        case "cores":
                                                            cores = Integer.parseInt(resAlloc);   break;
                                                        case "ram":
                                                            ram = Integer.parseInt(resAlloc);     break;
                                                    }
                                                }
                                            }
                                        };  break;
                                        case "resourceConsume":{
                                            NodeList resItems = taskItem.getChildNodes();
                                            for(int n = 0; n < resItems.getLength(); n++) {
                                                Node resItem = resItems.item(n);
                                                if(resItem instanceof Element){
                                                    String resCon = resItem.getFirstChild().getNodeValue().trim();
                                                    switch (resItem.getNodeName()) {
                                                        case "csps":
                                                            csps = Double.parseDouble(resCon);     break;
                                                    }
                                                }
                                            }

                                        };  break;
                                    }
                                }
                            }

                            // 有些变量在job中设置，但是task执行时需要
                            // 所以实际是将变量放入了task
                            Resource resource = new Resource(cores, ram);
                            resource.setScps(csps);
                            t.setResourceRequests(resource);
                            t.setJobId(job.getJobId());
                            taskList.add(t);
                            //System.out.println(t);
                        }
                    }
                }
            }
        }
        job.setTasks(taskList);
        for(Task task : taskList){
            task.setPriority(priority);
        }
        //System.out.println(job);
        LOG.info("Load job from xml. Job=" + job);
        return job;
    }

     /*public static void main(String[] args) {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        DocumentBuilder builder = null;

        Document document = null;
        try {
            builder = factory.newDocumentBuilder();
            document = builder.parse(new InputSource(new FileInputStream("D:\\job.xml")));
            System.out.println("read xml file.");
        } catch (ParserConfigurationException e) {
            LOG.error("Load job from XML error.");
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            LOG.error("Load job from XML error.");
            e.printStackTrace();
        } catch (IOException e) {
            LOG.error("Load job from XML error.");
            e.printStackTrace();
        } catch (SAXException e) {
            LOG.error("Load job from XML error.");
            e.printStackTrace();
        }
        new JobLoader().getModel(document);
    }*/


}
