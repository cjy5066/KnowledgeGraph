package service;

import analysis.ServiceAnalysis;
import dao.KnowledgeDao;
import database.ConnectionFormat;
import neo4j.ApocUtil;
import neo4j.CreateUtil;
import neo4j.DataQueryUtil;
import neo4j.MarkUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import util.ResultUtil;
import util.ShellUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KnowledgeService {
    @Autowired
    private KnowledgeDao dao;

    @Autowired
    private ServiceAnalysis analysis;

    @Autowired
    private ApocUtil apoc;

    @Autowired
    private CreateUtil create;

    @Autowired
    private MarkUtil mark;

//    private String port = "7687";

    /**
     * @Description 生成知识图谱
     * @Param [taskId]
     * @Return java.util.Map<java.lang.String, java.lang.String>
     * @Author zhangyunchao
     * @Date 2020/8/3
     * @Time 10:13
     */
    public Map<String, Object> createByDatabase(String taskId, String useway) {
        Map<String, Object> retMap = new HashMap<>();
        try {
            String port = getPort(taskId);
            updateToProcess(taskId);
            Map<String, Object> DBdata = getDBdata(taskId);
            String DBUrl = ConnectionFormat.getConnectionUrl(DBdata);
            List<Map<String, Object>> RDF = getRDF(taskId);
            analysis.analysis(RDF, getFilter(taskId), DBdata.get("DATABASETYPE").toString(), useway);
            retMap = apoc.createByJdbc(RDF, DBUrl, taskId, port, useway);
            updateToEnd(taskId);
        } catch (Exception e) {
            return ResultUtil.error("图谱生成失败");
        }
        return retMap;
    }

    public Map<String, Object> createByFile(String taskId) {
        Map<String, Object> retMap = new HashMap<>();
        try {
            updateToProcess(taskId);
            List<Map<String, Object>> schema = getSchema(taskId);
            Map<String, Object> infor = getPortAndEval(taskId);
            String port = infor.get("MAP_PORT") == null ? "7687" : infor.get("MAP_PORT").toString();
            retMap = create.createByFile(schema,port,taskId,infor.get("EVAL_ID").toString());
            updateToEnd(taskId);
        }catch (Exception e){
            return ResultUtil.error("图谱生成失败");
        }
        return retMap;
    }

    public String getPort(String taskId) {
        //科研用
//        return dao.getPort(taskId).get("MAP_PORT") == null ? "7687" : dao.getPort(taskId).get("MAP_PORT").toString();
        return dao.getPort(taskId).get("MAP_PORT") == null ? "" : dao.getPort(taskId).get("MAP_PORT").toString();
    }

    public Map<String, Object> createByMark(String taskId) {
        Map<String, Object> retMap = new HashMap<>();
        try {
            updateToProcess(taskId);
            String port = getPort(taskId);
            retMap = mark.createByMark(taskId, port);
            updateToEnd(taskId);
        }catch (Exception e){
            return ResultUtil.error("图谱生成失败");
        }
        return retMap;
    }

    public Map<String, Object> getPortAndEval(String taskId) {
        return dao.getPortAndEval(taskId);
    }

    public List<Map<String, Object>> getSchema(String taskId) {
        return dao.getSchema(taskId);
    }

    public void updateToProcess(String taskId) {
        dao.updateToProcess(taskId);
    }

    public void updateToEnd(String taskId) {
        dao.updateToEnd(taskId);
    }

    /**
     * @Description 获取当前任务数据源连接URL
     * @Param [taskId]
     * @Return java.lang.String
     * @Author zhangyunchao
     * @Date 2020/8/3
     * @Time 10:13
     */
    public Map<String, Object> getDBdata(String taskId) throws Exception {
        return dao.getDBdata(taskId);
    }

    /**
     * @Description 获取当前任务三元组信息
     * @Param [taskId]
     * @Return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
     * @Author zhangyunchao
     * @Date 2020/8/3
     * @Time 10:12
     */
    public List<Map<String, Object>> getRDF(String taskId) {
        return dao.getRDFList(taskId);
    }

    /**
     * @Description 获取当前任务过滤规则
     * @Param [taskId]
     * @Return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
     * @Author zhangyunchao
     * @Date 2020/8/3
     * @Time 10:12
     */
    public List<Map<String, Object>> getFilter(String taskId) {
        return dao.getFilterList(taskId);
    }

    public Map<String, Object> exportCsv(String taskId) {
        String port = getPort(taskId);
//        String port = "7687";
        if (port == null || "".equals(port)) {
            return ResultUtil.error("图谱导出失败!");
        }
        return apoc.exportCsv(port, taskId);
    }

    public Map<String, Object> createDocker(String taskId) {
        Map<String, Object> retMap = new HashMap<>();
        try {
            String port = getPort(taskId);
//            String port = "7687";
            if(port != null && !"".equals(port)){
                return ResultUtil.success("任务实例创建成功！");
            }else{
                port = new ShellUtil().createDocker(taskId);
//            port = "7687";
                if (!"error".equals(port)) {
                    dao.updatePort(taskId, port);
                    ResultUtil.success("任务实例创建成功！");
                } else {
                    return ResultUtil.error("任务实例创建失败！");
                }
            }
        } catch (Exception e) {
            return ResultUtil.error("任务实例创建失败！");
        }
        return retMap;
    }
}
