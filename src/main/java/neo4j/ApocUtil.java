package neo4j;

import neo4j.util.DriverFactory;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import util.ResultUtil;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class ApocUtil {
    private static Logger logger = LoggerFactory.getLogger(ApocUtil.class);
    //107
//    private final String url = "bolt://172.18.130.107:";
    //党政项目
//    private final String url = "bolt://172.18.194.8:";
//    private final String url = "bolt://172.18.194.203:";
//        private final String url = "bolt://192.168.150.55:";
//雄安内网
    private final String url = "bolt://10.49.35.11:";
    //本地
//    private final String url = "bolt://localhost:";

    /**
     * @Description 通过APOC中jdbc方式导入图谱数据
     * @Param [RDF, DBUrl]
     * @Return java.util.Map<java.lang.String                                                                                                                                                                                                                                                               ,                                                                                                                                                                                                                                                               java.lang.String>
     * @Author zhangyunchao
     * @Date 2020/8/3
     * @Time 9:47
     */
    public Map<String, Object> createByJdbc(List<Map<String, Object>> RDF, String DBUrl, String taskId, String port, String useway) {
        Driver driver = null;
        Session session = null;
        try {
            long starttime = System.currentTimeMillis();
            if ("7687".equals(port)) {   //党政项目用
//                driver = DriverFactory.getDriver(url, "7687");
                driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "Jufeng2010&"));
            } else {
                driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "Jufeng2010&"));
            }
            session = driver.session();
            for (Map<String, Object> Rdf : RDF) {
                String cypher = getCypher(Rdf, DBUrl, taskId, useway);
                logger.info("\n执行插入：" + cypher);
                long start = System.currentTimeMillis();
                if ("01".equals(useway)) {
                    session.run("CREATE INDEX ON :`" + Rdf.get("START_ALIAS") + "`(name)");
                    session.run("CREATE INDEX ON :`" + Rdf.get("END_ALIAS") + "`(name)");
                } else {
                    session.run("CREATE INDEX ON :`" + Rdf.get("S_TABLE_ALIAS") + "." + Rdf.get("START_ALIAS") + "`(name)");
                    session.run("CREATE INDEX ON :`" + Rdf.get("E_TABLE_ALIAS") + "." + Rdf.get("END_ALIAS") + "`(name)");
                }
                session.run(cypher);
//                Thread.sleep(1000);
                long end = System.currentTimeMillis();
                logger.info("\n本次插入耗时：" + (end - start) + "ms");
            }
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

    /**
     * @Description 拼接APOC执行的语句
     * @Param [Rdf, DBUrl]
     * @Return java.lang.String
     * @Author zhangyunchao
     * @Date 2020/8/3
     * @Time 9:45
     */
    public String getCypher(Map<String, Object> Rdf, String DBUrl, String taskId, String useway) {
        StringBuffer cypher = new StringBuffer();
        String sql = Rdf.get("SQL").toString();
        String param = "";
        Pattern p = Pattern.compile("'%.*?%'");
        Matcher m = p.matcher(sql);
        String allParam = "[]";
        int flag = 0;
        while (m.find()) {
            param = param + m.group().toString() + ",";
            System.out.println("param ： " + m.group());
            flag = 1;
        }
        if (flag == 1) {
            param = param.substring(0, param.lastIndexOf(","));
            allParam = "[" + param + "]";
            Rdf.put("SQL", Rdf.get("SQL").toString().replaceAll("'%.*?%'", "?"));
        }
        try {
            cypher.append("CALL apoc.periodic.iterate(\"CALL apoc.load.jdbc");
            cypher.append("('");
            cypher.append(DBUrl + "','");
            cypher.append(Rdf.get("SQL"));
            cypher.append("'," + allParam + ")\",");
            if ("01".equals(useway)) {
                cypher.append("\"MERGE (n:`" + Rdf.get("START_ALIAS") + "` {name:row." + Rdf.get("START_ORIGIN") + ", disaster:row.V_BAS_DISASTER}) with * ");
                cypher.append("MERGE (m:`" + Rdf.get("END_ALIAS") + "` {name:row." + Rdf.get("END_ORIGIN") + ", disaster:row.V_BAS_DISASTER}) with * ");
            } else {
                cypher.append("\"MERGE (n:`" + Rdf.get("S_TABLE_ALIAS") + "." + Rdf.get("START_ALIAS") + "` {name:row." + Rdf.get("START_ORIGIN") + "}) with * ");
                cypher.append("MERGE (m:`" + Rdf.get("E_TABLE_ALIAS") + "." + Rdf.get("END_ALIAS") + "` {name:row." + Rdf.get("END_ORIGIN") + "}) with * ");
            }
            cypher.append("MERGE (n)-[r:`" + Rdf.get("REL") + "` {name:'" + Rdf.get("REL") + "'}]->(m)\"");
            cypher.append(",{batchSize:10000,iterateList:true})");
        } catch (NullPointerException e) {
            logger.error("APOC语句拼接失败：" + e.getMessage());
        }
        return cypher.toString();
    }

    public Map<String, Object> exportCsv(String port, String taskId) {
        Driver driver = null;
        Session session = null;
        try {
            String cypher = "CALL apoc.export.csv.all('" + taskId + ".csv',{stream:true,batchSize:20000})";
            driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "neo4j"));
            session = driver.session();
            session.run(cypher);
            return ResultUtil.success("图谱导出成功");
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.error("图谱导出失败！");
        } finally {
            session.close();
            driver.close();
        }
    }
}
