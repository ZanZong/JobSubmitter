package ict.zongzan.util;

import java.io.FileWriter;
import java.io.IOException;

/**
 * 当job数量众多时，配置比较麻烦，使用该程序生成脚本
 * 在开发环境中生成，复制到运行环境即可
 * Created by Zongzan on 2016/12/19.
 */
public class GenerateJobXML {
    private String filePath = "";

    public GenerateJobXML(String filePath) {
        this.filePath = filePath;
    }
    private void generateJob(){

    }

    private void generate(){
        FileWriter writer = null;
        try{
            writer = new FileWriter(filePath);
            writer.write("<jobs>");
            writer.write("</jobs>");
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args){
        GenerateJobXML generateJobXML = new GenerateJobXML("D:\\job.xml");
        generateJobXML.generate();
    }

    class JobModel{
        
    }
}
