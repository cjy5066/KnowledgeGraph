package neo4j;

import neo4j.query.DijkstraAnalysis;
import neo4j.query.LabelAnalysis;
import neo4j.query.ResultAnalysis;
import neo4j.util.DriverFactory;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import util.ResultUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class DijkstraUtil {
    private static Logger logger = LoggerFactory.getLogger(DataQueryUtil.class);
//    private final String url = "bolt://172.18.194.8:";
//    private final String url = "bolt://172.18.130.107:";
    //private final String url = "bolt://172.18.194.203:";
//    private final String url = "bolt://192.168.150.55:";
    //雄安内网
    private final String url = "bolt://10.49.35.11:";
    //本地
//    private final String url = "bolt://localhost:";

    public Map<String, Object> getShortestPath(Map<String, Object> param, String port) {
//        port = "7687";
        Driver driver = null;
        Session session = null;
        List<Map<String, Object>> categories = new ArrayList<>();
        Map<String, Object> retMap = new HashMap<>();
        try {
            if (param.get("disaster") != null && !"".equals(param.get("disaster"))) {
                driver = DriverFactory.getDriver(url, "11012");
            } else if ("7687".equals(port)) {   //党政项目用
                driver = GraphDatabase.driver("bolt://172.18.194.215:" + port, AuthTokens.basic("neo4j", "Jufeng2010&"));
//                driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "Jufeng2010&"));
            } else {
                driver = GraphDatabase.driver(url + port, AuthTokens.basic("neo4j", "Jufeng2010&"));
            }
            session = driver.session();
            if (param.get("currentPage") == null || "".equals(param.get("currentPage"))) {
                param.put("currentPage", 1);
            }
            if (param.get("pageSize") == null || "".equals(param.get("pageSize"))) {
                param.put("pageSize", 10);
            }
            int startIndex = (Integer.parseInt(param.get("currentPage").toString()) - 1) * Integer.parseInt(param.get("pageSize").toString());
            int endIndex = Integer.parseInt(param.get("pageSize").toString());
            String cypher = "";
            int flag = 0;
            if (param.get("mid") != null && !"".equals(param.get("mid"))) {
                flag = 1;
                cypher = getCypherThree(param);
            } else {
                cypher = getCypherTwo(param);
            }
            logger.info("最短路径查询：" + cypher);
            StatementResult result = session.run(cypher, Values.parameters("start", param.get("start"), "end", param.get("end"), "startIndex", startIndex, "endIndex", endIndex, "disaster", param.get("disaster"), "mid", param.get("mid")));
//            int count = session.run(getResultCount(param), Values.parameters("start", param.get("start"), "end", param.get("end"), "disaster", param.get("disaster"))).list().get(0).get("COUNT(p)").asInt();
            categories = new LabelAnalysis().getLabels(session);
            retMap = new DijkstraAnalysis().getResult(result, categories, flag);
            retMap.put("totalRecord", 10);
            retMap.putAll(ResultUtil.success("图谱查询成功!"));
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.error("图谱查询失败!");
        } finally {
            session.close();
            if ((param.get("disaster") == null || "".equals(param.get("disaster"))) && !"7687".equals(port)) {
                driver.close();
            }
        }
        return retMap;
    }

    public String getCypherTwo(Map<String, Object> param) {
        if (param.get("disaster") != null && !"".equals(param.get("disaster"))) {
            return "MATCH p1 = allShortestPaths((n {name: {start}})-[*..10]-(m {name: {end}})) WHERE n.disaster = {disaster} and m.disaster = {disaster} and all(x in nodes(p1) where not (x:`受灾人口(人)` or x:`受伤人口(人)` or x:`失踪人口(人)` or x:`死亡人口(人)` or x:`被困人口(人)` or x:`转移安置人口` or x:`饮水困难人口(人)` or x:`修改时间` or x:`入库时间` or x:`填表时间` or x:`收到时间` or x:`资料时间` or x:`农作物受灾面积(公顷)` or x:`农作物成灾面积(公顷)` or x:`农作物绝收面积(公顷)` or x:`林业受灾面积(公顷)` or x:`渔业影响面积(公顷)` or x:`牧草受灾面积(公顷)` or x:`当地近年耕地面积(公顷)`) ) RETURN p1 SKIP {startIndex} LIMIT {endIndex}";
        } else {
            return "MATCH p1 = allShortestPaths((n {name: {start}})-[*..10]-(m {name: {end}})) RETURN p1 SKIP {startIndex} LIMIT {endIndex}";
        }
    }

    public String getCypherThree(Map<String, Object> param) {
        //例子（match  (m {name:'昌江区'}),(n {name:'360000'}),(s {name:'桂东县'}) where m.disaster='暴雨洪涝' and n.disaster='暴雨洪涝' and s.disaster='暴雨洪涝'  with m,n,s match p1 = shortestpath((m)-[*..10]-(n)) match p2 = shortestpath((n)-[*..10]-(s)) return p1, p2）
        if (param.get("disaster") != null && !"".equals(param.get("disaster"))) {
            return "match  (m {name: {start}}),(n {name: {mid}}),(s {name: {end}}) where m.disaster={disaster} and n.disaster={disaster} and s.disaster={disaster}  with m,n,s match p1 = shortestpath((m)-[*..10]-(n)) match p2 = shortestpath((n)-[*..10]-(s)) return p1, p2 order by length(p1)+length(p2) SKIP {startIndex} LIMIT {endIndex}";
        } else {
            return "match  (m {name: {start}}),(n {name: {mid}}),(s {name: {end}})  with m,n,s match p1 = shortestpath((m)-[*..10]-(n)) match p2 = shortestpath((n)-[*..10]-(s)) return p1, p2  order by length(p1)+length(p2) SKIP {startIndex} LIMIT {endIndex}";
        }
    }

    public String getResultCount(Map<String, Object> param) {
        if (param.get("disaster") != null && !"".equals(param.get("disaster"))) {
            return "MATCH p = allShortestPaths((n {name: {start}})-[*..10]-(m {name: {end}})) WHERE n.disaster = {disaster} and m.disaster = {disaster} and all(x in nodes(p1) where not (x:`受灾人口(人)` or x:`受伤人口(人)` or x:`失踪人口(人)` or x:`死亡人口(人)` or x:`被困人口(人)` or x:`转移安置人口` or x:`饮水困难人口(人)` or x:`修改时间` or x:`入库时间` or x:`填表时间` or x:`收到时间` or x:`资料时间` or x:`农作物受灾面积(公顷)` or x:`农作物成灾面积(公顷)` or x:`农作物绝收面积(公顷)` or x:`林业受灾面积(公顷)` or x:`渔业影响面积(公顷)` or x:`牧草受灾面积(公顷)` or x:`当地近年耕地面积(公顷)`) ) RETURN COUNT(p)";
        } else {
            return "MATCH p = allShortestPaths((n {name: {start}})-[*..10]-(m {name: {end}})) RETURN COUNT(p)";
        }
    }
}
