package controller;

import neo4j.dbutils.Neo4jDbUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import service.QueryService;
import sun.applet.Main;
import util.ResultUtil;

import java.util.HashMap;
import java.util.Map;

@RestController
@CrossOrigin
public class QueryController {

    @Autowired
    private QueryService query;

    //http://10.49.35.11:8181/knowledge/show?taskId=111111&enty=%E6%B0%94%E8%B1%A1%E4%B8%93%E4%B8%9A%E7%9F%A5%E8%AF%86%E5%9B%BE%E8%B0%B1
    // http://127.0.0.1:8181/knowledge/show?taskId=111111&enty=%E6%B0%94%E8%B1%A1%E4%B8%93%E4%B8%9A%E7%9F%A5%E8%AF%86%E5%9B%BE%E8%B0%B1
    @GetMapping("/show")
    public Map<String, Object> showKnowledgeMap(@RequestParam(value = "taskId", required = true) String taskId,
                                                @RequestParam(value = "label", required = false) String label,
                                                @RequestParam(value = "entity", required = false) String entity,
                                                @RequestParam(value = "level", required = false) Integer level,
                                                @RequestParam(value = "count", required = false) Integer count,
                                                @RequestParam(value = "disaster", required = false) String disaster,
                                                @RequestParam(value = "enty", required = false) String  enty) throws Exception {
        System.out.println("来了");
        String id = "";
        if(entity == null || "".equals(entity) || " ".equals(entity)){
            if(enty.contains("专业知识")){
                enty = "专业知识";
                id = "111111";
            }else if(enty.contains("数据基础")){
                enty = "数据基础";
                id = "222222";
            }else if(enty.contains("算法支撑")){
                enty = "算法支撑";
                id = "333333";
            }else if(enty.equals("雄安知识图谱")){
                enty = "雄安知识图谱";
            }else if(enty.equals("气象服务产品图谱")){
                enty = "气象服务产品";
                id = "8ab0a7377aa93205017aa94446520001";
            }
            entity = enty;
        }

        // 层级数默认为2
        if(level == null || "".equals(level)){
            level = 2;
        }

        if(count == null || "".equals(count)){
            count = 9999;
        }
        Map<String, Object> param = new HashMap<>();
        param.put("taskId", taskId);
        param.put("count", count);
        param.put("level", level);
        param.put("entity", entity);
        param.put("disaster", disaster);
        param.put("id", id);
        Map<String, Object> retMap = query.showKnowledgeMap(param);
        return retMap;
    }

    @GetMapping("/query/shortest")
    public Map<String, Object> showShortestPath(@RequestParam(value = "taskId") String taskId,
                                                @RequestParam(value = "start") String start,
                                                @RequestParam(value = "end") String end,
                                                @RequestParam(value = "mid", required = false) String mid,
                                                @RequestParam(value = "currentPage", required = false) Integer currentPage,
                                                @RequestParam(value = "pageSize", required = false) Integer pageSize,
                                                @RequestParam(value = "disaster", required = false) String disaster) throws Exception {
        Map<String, Object> retMap  = new HashMap<>();
        Map<String, Object> param = new HashMap<>();
        param.put("taskId", taskId);
        param.put("start", start);
        param.put("end", end);
        param.put("mid", mid);
        param.put("currentPage", currentPage);
        param.put("pageSize", pageSize);
        param.put("disaster", disaster);
        if("".equals(start) || "".equals(end)){
            return ResultUtil.error("图谱查询失败！");
        }else {
            retMap = query.queryShortestPath(param);
        }
        return retMap;
    }




    @GetMapping("/query/tree")
    public Map<String, Object> showShortestPathtree(@RequestParam(value = "tagName") String tagName,
                                                @RequestParam(value = "tagid") String tagid,

                                                @RequestParam(value = "grad", required = false) Integer grad
                                                ) throws Exception {

        if(tagName == null || "".equals(tagName)){
            tagName = "雄安知识图谱";
        }
        if(tagid == null || "".equals(tagid)){
            tagid = "1";
        }
        if(grad == null || grad <= 0){
            grad = 99; // 默认为99级，也就是查询全部
        }
        String  sql = "MATCH (ee:"+tagName+") WHERE ee.id = '"+tagid+"' RETURN ee";
        System.out.println(sql);

        Map<String, Object> map = new HashMap<>();

        Map<String, Object> retMap = Neo4jDbUtils.queryData(map, sql);


        return retMap;
    }

    public static void main(String[] args) {
        System.out.println("sdfafs");
        System.out.println("sdfafs");
        System.out.println("sdfafs");
        System.out.println("sdfafs");
        System.out.println("sdfafs");
        System.out.println();
    }
}
