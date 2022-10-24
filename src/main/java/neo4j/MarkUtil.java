package neo4j;

import com.alibaba.fastjson.JSONObject;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import util.ResultUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class MarkUtil {
    private static Logger logger = LoggerFactory.getLogger(CreateUtil.class);
    //公司服务器
//    private final String url = "bolt://172.18.130.107:";
    //private final String url = "bolt://172.18.194.203:";
//    private final String url = "bolt://192.168.150.55:";
    //北京气象灾害知识应用
//    private final String filePath = "/data/HKKS/JBOSS/JBoss4.2-hkks/server/default/deploy/HKKnowledgeStudio.war/TaskFile/QuickLabels";
//    //党政项目（公司）
    private final String url = "bolt://172.18.194.8:";
    private final String filePath = "/usr/local/jboss/JBoss4.2-hkks/server/default/deploy/HKKnowledgeStudio.war/TaskFile/QuickLabels";

    //雄安气象
//    private final String url = "bolt://10.49.35.11:";
    //本地
//    private final String url = "bolt://localhost:";
//    private final String filePath = "/usr/local/JBoss/JBoss4.2_HKKnowledgeStudio/server/default/deploy/HKKSMySQL.war/TaskFile/QuickLabels";

    public Map<String, Object> createByMark(String taskId, String port) {
        Driver driver = null;
        Session session = null;
        String labelPath = filePath + File.separator + taskId + File.separator + "schema";
        String jsonPath = filePath + File.separator + taskId + File.separator + "target";
        Map<String, String> labels = new HashMap<>();
        FileInputStream in = null;
        List<Map<String, Object>> sentences = new ArrayList<>();
        try {
            long starttime = System.currentTimeMillis();
            //107服务器
//            driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "Jufeng2010&"));
            //党政项目（公司）
            driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "neo4j"));
            session = driver.session();
            getLabels(labelPath + File.separator + "connectionCategories.json", labels, in);
            getLabels(labelPath + File.separator + "labelCategories.json", labels, in);
            createIndex(session, labels);
            File dir = new File(jsonPath);
            String content = "";
            List<Map<String, Object>> nodes = new ArrayList<>();
            List<Map<String, Object>> relationships = new ArrayList<>();
            getAllFile(in, dir, content, session, nodes, relationships, labels, sentences);
            long endtime = System.currentTimeMillis();
            logger.info("生成耗时：" + (endtime - starttime) + " ms");
            return ResultUtil.success("知识图谱生成成功");
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.success("知识图谱生成失败");
        } finally {
            session.close();
            driver.close();
        }
    }

    public void getAllFile(InputStream in, File dir, String content, Session session, List<Map<String, Object>> nodes, List<Map<String, Object>> relationships, Map<String, String> labels, List<Map<String, Object>> sentences) {
        File[] fileList = dir.listFiles();
        for (File f : fileList) {
            if (f.isDirectory()) {
                getAllFile(in, f, content, session, nodes, relationships, labels, sentences);
            } else {
                try {
                    Long filelength = f.length();
                    byte[] filecontent = new byte[filelength.intValue()];
                    in = new FileInputStream(f);
                    in.read(filecontent);
                    String json = new String(filecontent, "utf-8");
                    Map result = JSONObject.parseObject(json);
                    sentences = (List<Map<String, Object>>) result.get("sentences");
                    for (Map<String, Object> sentence : sentences) {
                        Map<String, Object> annotation = (Map<String, Object>) sentence.get("annotation");
                        content = annotation.get("content").toString();
                        nodes = (List<Map<String, Object>>) annotation.get("labels");
                        relationships = (List<Map<String, Object>>) annotation.get("connections");
                        if (nodes != null && nodes.size() > 0) {
                            CreateUtil.createKnowledgeMap(content, session, nodes, relationships, labels);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (in != null) {
                            in.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }

    public void createIndex(Session session, Map<String, String> labels) {
        try {
            for (String value : labels.values()) {
                session.run("CREATE INDEX ON :`" + value + "`(name)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getLabels(String labelPath, Map<String, String> labels, InputStream in) {
        File f = new File(labelPath);
        try {
            Long filelength = f.length();
            byte[] filecontent = new byte[filelength.intValue()];
            in = new FileInputStream(f);
            in.read(filecontent);
            String json = new String(filecontent, "utf-8");
            List list = JSONObject.parseArray(json);
            for (Object item : list) {
                Map<String, String> map = (Map<String, String>) item;
                labels.put(map.get("id"), map.get("text").toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
