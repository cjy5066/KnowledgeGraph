package neo4j.dbutils;

import neo4j.query.LabelAnalysis;
import neo4j.query.ResultAnalysis;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import util.ResultUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.LoggerFactory;

@Component
public class Neo4jDbUtils {
    private static Logger logger =  LoggerFactory.getLogger(Neo4jDbUtils.class);
    private static final String url = "bolt://10.49.35.11:";
    private static final String port = "7474";

    //本地
//    private final String url = "bolt://localhost:";

    public static Map<String, Object> queryData(Map<String, Object> param,String sql) {
        long starttime = System.currentTimeMillis();
        Driver driver = null;
        Session session = null;
        List<Map<String, Object>> categories = new ArrayList<>();
        Map<String, Object> retMap = new HashMap<>();
        try {
            driver = GraphDatabase.driver(url + "7687", AuthTokens.basic("neo4j", "Jufeng2010&"));

            session = driver.session();

            StatementResult result = session.run(sql);
            long middletime = System.currentTimeMillis();
            logger.info("查询耗时： " + (middletime - starttime));
            categories = new LabelAnalysis().getLabels(session);
            retMap = new ResultAnalysis().getResult(result, categories, param);
            retMap.putAll(ResultUtil.success("图谱查询成功!"));
            long endtime = System.currentTimeMillis();
            logger.info("总共耗时：" + (endtime - starttime));
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.error("图谱查询失败!");
        } finally {
            long s = System.currentTimeMillis();
            session.close();
            long m = System.currentTimeMillis();
            logger.info("session关闭耗时：" + (m - s));
            if ((param.get("disaster") == null || "".equals(param.get("disaster"))) && !"7687".equals(port)) {
                driver.close();
            }
            driver.close();
            long e = System.currentTimeMillis();
            logger.info("driver关闭耗时：" + (e - m));
        }
        return retMap;
    }

}
