package service;

import dao.KnowledgeDao;
import neo4j.DataQueryUtil;
import neo4j.DijkstraUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import util.ResultUtil;

import java.util.Map;

@Service
public class QueryService {
    @Autowired
    private KnowledgeDao dao;


    @Autowired
    private DataQueryUtil query;

    @Autowired
    private DijkstraUtil dijkstra;

    private String port = "7687";

    public Map<String, Object> showKnowledgeMap(Map<String, Object> param) throws Exception {
//        port = getPort(param.get("taskId").toString());
//        if (port == null || "".equals(port)) {
//            return ResultUtil.error("图谱查询失败!");
//        }
        return query.queryData(param, "7474");
    }

    public Map<String, Object> queryShortestPath(Map<String, Object> param) throws Exception {
        port = getPort(param.get("taskId").toString());
        if (port == null || "".equals(port)) {
            return ResultUtil.error("图谱查询失败!");
        }
        return dijkstra.getShortestPath(param, port);
    }

    public String getPort(String taskId) {
        //科研用
        return dao.getPort(taskId).get("MAP_PORT") == null ? "7687" : dao.getPort(taskId).get("MAP_PORT").toString();
//        return dao.getPort(taskId).get("MAP_PORT") == null ? "" : dao.getPort(taskId).get("MAP_PORT").toString();
    }
}
