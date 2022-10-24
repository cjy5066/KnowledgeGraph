package neo4j;

import neo4j.query.LabelAnalysis;
import neo4j.query.ResultAnalysis;
import neo4j.util.DriverFactory;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import util.CJYCollectionUtils;
import util.ResultUtil;


import java.util.*;
import java.util.stream.Collectors;

@Component
public class DataQueryUtil {
    private static Logger logger = LoggerFactory.getLogger(DataQueryUtil.class);
    private  Map<String, List<Map<String, Object>>> belongtoMap = null;
    private Map<String, Map<String, Object>> idMaps = null;
    private  Set<Map<String, Object>> links = null;
    private  List<Map<String, Object>> categories = null;
    private  Map<String,Map<String,Object>> overallNodes = null;
//    private  Set<Map<String,Object>>  setNodes = null;

    private int level = 2;
    private int count = 9999;

    //107
//    private final String url = "bolt://172.18.130.107:";
    //党政
//    private final String url = "bolt://172.18.194.8:";
    //    private final String url = "bolt://192.168.150.55:";
    //雄安内网
    private final String url = "bolt://10.49.35.11:";
    //本地
//    private final String url = "bolt://localhost:";
    private String password = "Jufeng2010&";
//    private String password = "5066";

    public Map<String, Object> queryData(Map<String, Object> param, String port) {
//        port = "7687";
        long starttime = System.currentTimeMillis();
        Driver driver = null;
        Session session = null;
        List<Map<String, Object>> categories = new ArrayList<>();
        Map<String, Object> retMap = new HashMap<>();
        try {

            driver = GraphDatabase.driver(url + "7687", AuthTokens.basic("neo4j", password));

            session = driver.session();
            System.out.println("链接得到上年AAAAAAAAAA==========================");
            /*
            查询所有的节点
             */
            long s = System.currentTimeMillis();
            ResultAnalysis resultAnalysis = new ResultAnalysis();
            StatementResult allresult = session.run("MATCH (n) RETURN n");
            ResultAnalysis re = new ResultAnalysis();
            while (allresult.hasNext()) {
                Record record = allresult.next();
                long n = record.get("n").asNode().id();

                re.getNodes(n,"n",record,categories, param);
            }
            List<Map<String, Object>> nodes = re.getNodes();
            System.out.println("全部的节点数量： "+nodes.size());
            belongtoMap = CJYCollectionUtils.groupListMap(nodes, "belongto");





            /*
                获取当前要查询的节点
             */
            Object id = param.get("id");
            Object entity = param.get("entity");
            level = Integer.parseInt(param.get("level")+"");
            count = Integer.parseInt(param.get("count")+"");
            System.out.println("level 层级数： "+level+"    count 总数："+count);

            String  sql = "";

            if("".equals(id)){
                sql = "match (n) where n.name =~'.*"+entity+".*'   return n";
            }else{
                sql = "match (n) where n.name =~'.*"+entity+".*'  and n.id ='"+id+"'   return n";
            }
            System.out.println("当前sql： "+sql);
            StatementResult run = session.run(sql);
            ResultAnalysis thisre = new ResultAnalysis();
            while (run.hasNext()) {
                Record record = run.next();
                long n = record.get("n").asNode().id();

                thisre.getNodes(n,"n",record,categories, param);
            }
            List<Map<String, Object>> thisNodes = thisre.getNodes();
            System.out.println("thisNodes: "+thisNodes.size());


            overallNodes = new HashMap<String,Map<String,Object>>();
            this.links = new HashSet<Map<String,Object>>();
            this.categories = new ArrayList<Map<String, Object>>();

            /*
                这个if是设计到多个模糊查询；模糊查询会查到很多节点，然后再查这些节点的所有子和所有父

             */
            if(thisNodes.size() > 1){
                idMaps = CJYCollectionUtils.listToIdGetMap(nodes, "key");
//                setNodes = new HashSet<Map<String,Object>>();
                for(int i=0; i<thisNodes.size(); i++){
                    Map<String, Object> nodesMap = thisNodes.get(i);
                    String belongto = nodesMap.get("belongto")+"";
                    RecursiveBelongto(belongto);

                    // 给当前节点 ->父节点 的关系
                    Map<String, Object> stringObjectMap = idMaps.get(belongto);
                    HashMap<String, Object> linksMap = new HashMap<>();
                    linksMap.put("rel","belongt");
                    linksMap.put("target",stringObjectMap.get("name"));  // 父节点neo4j自带id
                    linksMap.put("source",nodesMap.get("name")); // 当前节点的neo4j自带id
                    links.add(linksMap);
                }

//                for (Map<String,Object> setmap : setNodes  ) {
//                    overallNodes.add(setmap);
//                }
            }




                for (int i = 0; i < thisNodes.size(); i++) {
                    Map<String, Object> nodesMap = thisNodes.get(i);
                    String belongto = nodesMap.get("key") + "";
                    overallNodes.put(belongto,nodesMap);
//                    Map<String, Object> camap = new HashMap<String, Object>();
//                    camap.put("name", nodesMap.get("nodeName"));
//                    this.categories.add(camap);

                    String nameid = nodesMap.get("name") + "";
                    Recursive(belongto, nameid, level);
                }



                ArrayList<Map<String, Object>> maps = new ArrayList<>();

            int h =0;
            for (String key:overallNodes.keySet() ) {
                Map<String, Object> map = overallNodes.get(key);

                Map<String, Object> new_map = new HashMap<String,Object>();
                new_map.putAll(map);
                new_map.put("category",h);
                maps.add(new_map);

                Map<String, Object> camap = new HashMap<String,Object>();
                camap.put("name",new_map.get("nodeName"));
                this.categories.add(camap);

                h++;
            }
//                for(int h=0;h<overallNodes.size();h++){
//                    Map<String, Object> map = overallNodes.get(h);
//                    Map<String, Object> new_map = new HashMap<String,Object>();
//                    new_map.putAll(map);
//                    new_map.put("category",h);
//                    maps.add(new_map);
//
//                    Map<String, Object> camap = new HashMap<String,Object>();
//                    camap.put("name",new_map.get("nodeName"));
//                    this.categories.add(camap);
//                }

                HashMap<Object, Object> dataMap = new HashMap<>();
                dataMap.put("nodes",maps);
                dataMap.put("links",this.links);
                dataMap.put("categories",this.categories);
                System.out.println("nodes 数量："+overallNodes.size()+"    links 数量："+this.links.size()+"ca : "+this.categories.size());


//                    StatementResult result = session.run(getCypher(param));
//                    LabelAnalysis labelAnalysis = new LabelAnalysis();
//                    categories = labelAnalysis.getLabels(session);
//                    retMap = resultAnalysis.getResult(result, categories, param);
                retMap.put("data",dataMap);
                retMap.putAll(ResultUtil.success("图谱查询成功!"));



        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.error("图谱查询失败!");
        } finally {
            long s = System.currentTimeMillis();
            session.close();
            long m = System.currentTimeMillis();
            if ((param.get("disaster") == null || "".equals(param.get("disaster"))) && !"7687".equals(port)) {
                driver.close();
            }
//            driver.close();
//            long e = System.currentTimeMillis();
//            logger.info("driver关闭耗时：" + (e - m));
        }
        return retMap;
    }


    public String getCypher(Map<String, Object> param) {
        StringBuffer cypher = new StringBuffer();
        cypher.append("match p = (n)-[r*1..");
        if (param.get("level") != null) {
            cypher.append(param.get("level")).append("]-(m) ");
        } else {
            cypher.append("2]-(m) ");
        }
        if (param.get("entity") != null && !"".equals(param.get("entity"))) {
            cypher.append("where n.name =~'.*").append(param.get("entity")).append(".*' ");
            if (param.get("disaster") != null && !"".equals(param.get("disaster"))) {
                cypher.append(" and n.disaster ='").append(param.get("disaster")).append("' and m.disaster='").append(param.get("disaster")).append("'");
            }
            if(param.get("id" ) != null && !"".equals(param.get("id"))){
                cypher.append(" and n.id ='").append(param.get("id")).append("' ");;
            }
        } else if (param.get("disaster") != null && !"".equals(param.get("disaster"))) {
            cypher.append("where n.disaster ='").append(param.get("disaster")).append("' and m.disaster='").append(param.get("disaster")).append("'");
        }
        cypher.append(" return n,m,r limit ");
        if (param.get("count") != null) {
            cypher.append(param.get("count"));
        } else {
            cypher.append("600");
        }
        logger.info("查询语句：" + cypher.toString());
        System.out.println("查询语句换成：  MATCH (n) RETURN n");
        return cypher.toString();
    }


    public static void main(String[] args) {

        HashSet<String> strings = new HashSet<>();
        for(int i=0; i<=10;i++){
            strings.add(i+"");
        }
        for (String s: strings  ) {
            System.out.println(s);

        }

    }


    /**
     *
     * @param belongto
     * @param target     neo4j自带的id
     * @param level     层级
     */
    public void Recursive(String belongto,String target ,int level){

        List<Map<String, Object>> maps = belongtoMap.get(belongto);
        try{

                if(maps !=null){
                    if(level > 0){
                        level--;
                    for(int i=0; i<maps.size(); i++){
                            if(overallNodes.size() >count){
                               break;
                            }
                            Map<String, Object> mapNode = maps.get(i);
                            String belongtoId = mapNode.get("key")+"";
                            overallNodes.put(belongtoId,mapNode);

                            HashMap<String, Object> linksMap = new HashMap<>();
                            linksMap.put("rel","belongt");
                            linksMap.put("target",target);  // 父节点id
                            linksMap.put("source",mapNode.get("name")); // 当前节点id
                            links.add(linksMap);

//                            Map<String, Object> camap = new HashMap<String,Object>();
//                            camap.put("name",mapNode.get("nodeName"));
//                            categories.add(camap);

                            String nameid = mapNode.get("name")+"";
                            Recursive(belongtoId,nameid,level);
                        }
                    }
                }

        }catch (Exception e){
            e.printStackTrace();
        }

    }


    /**
     * 根据父节点id 一直往找
     * @param belongto
     */
    private void RecursiveBelongto(String belongto){
        Map<String, Object> belongtoMap = idMaps.get(belongto);
        if(belongtoMap == null ){
            return ;
        }

        //  添加节点
        String key = belongtoMap.get("key")+"";
        overallNodes.put(key,belongtoMap);

        // 父节点id
        String  id = belongtoMap.get("belongto")+"";
        if(id == null || "null".equals(id)){
            return ;
        }
        // 父节点map
        Map<String, Object> stringObjectMap = idMaps.get(id);

        HashMap<String, Object> linksMap = new HashMap<>();
        linksMap.put("rel","belongt");
        linksMap.put("target",stringObjectMap.get("name"));  // 父节点id
        linksMap.put("source",belongtoMap.get("name")); // 当前节点id
        links.add(linksMap);


        RecursiveBelongto(id);
    }
}
