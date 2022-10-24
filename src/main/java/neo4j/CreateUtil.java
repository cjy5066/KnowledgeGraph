package neo4j;

import com.alibaba.fastjson.JSONObject;
import neo4j.util.DriverFactory;
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
public class CreateUtil {
    private static Logger logger = LoggerFactory.getLogger(CreateUtil.class);
    //107
//    private final String url = "bolt://172.18.130.107:";
    //党政
    private final String url = "bolt://172.18.194.8:";
    //    private final String url = "bolt://172.18.194.203:";
    //雄安内网
//    private final String url = "bolt://10.49.35.11:";
    //本地
//    private final String url = "bolt://localhost:";
    //    private final String filePath = "C:\\HKKSMaven\\JBoss4.2\\server\\default\\deploy\\HKKnowledgeStudio.war\\TaskFile\\Train\\Json";
    //北京气象灾害知识应用
//    private final String filePath = "/data/HKKS/JBOSS/JBoss4.2-hkks/server/default/deploy/HKKnowledgeStudio.war/TaskFile/Train/Json";
//    private final String filePath = "/data/jboss/jboss_knowledge/server/default/deploy/HKKnowledgeStudio.war/TaskFile/Train/Json";
    //党政（产品）
    private final String filePath = "/usr/local/jboss/JBoss4.2-hkks/server/default/deploy/HKKnowledgeStudio.war/TaskFile/Train/Json";
    //雄安气象
//    private final String filePath = "/usr/local/jboss/JBoss4.2-mysql/server/default/deploy/HKKSMySQL.war/TaskFile/Train/Json";

    //领域知识图谱构建工具
//    private final String filePath = "/usr/local/JBoss/JBoss4.2_HKKnowledgeStudio/server/default/deploy/HKKSMySQL.war/TaskFile/Train/Json";

    public Map<String, Object> createByFile(List<Map<String, Object>> schema, String port, String taskId, String evalId) {
        Driver driver = null;
        Session session = null;
        String path = filePath + File.separator + taskId + File.separator + evalId;
        try {
            long starttime = System.currentTimeMillis();
            if ("7687".equals(port)) {   //党政项目用
                driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "neo4j"));
//                driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "Jufeng2010&"));
            } else {
                driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "neo4j"));
            }
            session = driver.session();
            Map<String, String> labels = createIndex(session, schema);
            File dir = new File(path);
            FileInputStream in = null;
            String content = "";
            List<Map<String, Object>> nodes = new ArrayList<>();
            List<Map<String, Object>> relationships = new ArrayList<>();
            getAllFile(in, dir, content, session, nodes, relationships, labels);
            long endtime = System.currentTimeMillis();
            logger.info("生成耗时：" + (endtime - starttime) + " ms");
            return ResultUtil.success("知识图谱生成成功");
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.success("知识图谱生成失败");
        } finally {
            session.close();
            if (!"7687".equals(port)) {
                driver.close();
            }
        }
    }

    public void getAllFile(InputStream in, File dir, String content, Session session, List<Map<String, Object>> nodes, List<Map<String, Object>> relationships, Map<String, String> labels) {
        File[] fileList = dir.listFiles();
        for (File f : fileList) {
            if (f.isDirectory()) {
                getAllFile(in, f, content, session, nodes, relationships, labels);
            } else {
                try {
                    System.out.println("fileNmae: " + f.getName());
                    Long filelength = f.length();
                    byte[] filecontent = new byte[filelength.intValue()];
                    in = new FileInputStream(f);
                    in.read(filecontent);
                    String json = new String(filecontent, "utf-8");
                    Map result = JSONObject.parseObject(json);
                    content = result.get("content").toString();
                    nodes = (List<Map<String, Object>>) result.get("labels");
                    relationships = (List<Map<String, Object>>) result.get("connections");
                    createKnowledgeMap(content, session, nodes, relationships, labels);
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


    public Map<String, String> createIndex(Session session, List<Map<String, Object>> schema) {
        Map<String, String> labels = new HashMap<>();
        try {
            long start = System.currentTimeMillis();
            for (Map<String, Object> label : schema) {
                session.run("CREATE INDEX ON :`" + label.get("SCHEMA_NAME") + "`(name)");
                labels.put(label.get("SCHEMA_ID").toString(), label.get("SCHEMA_NAME").toString());
            }
            long end = System.currentTimeMillis();
            logger.info("\n索引建立耗时：" + (end - start) + "ms");
            System.out.println("labels: " + labels);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return labels;
    }

    public static void createKnowledgeMap(String content, Session session, List<Map<String, Object>> nodes, List<Map<String, Object>> relationships, Map<String, String> labels) {
        try {
            Map<String, String> temp = new HashMap<>();
            for (Map<String, Object> node : nodes) {
                System.out.println("node.get(\"categoryId\") : " + node.get("categoryId"));
                String nodeValue = content.substring(Integer.parseInt(String.valueOf(node.get("startIndex"))), Integer.parseInt(String.valueOf(node.get("endIndex"))));
                String cypher = "`" + labels.get(node.get("categoryId")) + "` {name:'" + nodeValue + "'})";
                session.run("MERGE (n:" + cypher);
                System.out.println("node: " + "MERGE (n:" + cypher);
                temp.put(node.get("id").toString(), cypher);
            }
            for (Map<String, Object> rel : relationships) {
                String cypher = "MATCH (n:" + temp.get(rel.get("fromId").toString()) + "," + "(m:" + temp.get(rel.get("toId").toString()) + " MERGE (n)-[r:`" + labels.get(rel.get("categoryId")) + "` {name:'" + labels.get(rel.get("categoryId")) + "'}]->(m)";
                System.out.println("rel: " + cypher);
                session.run(cypher);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
