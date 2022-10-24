package neo4j.query;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import java.util.*;
import java.util.stream.Collectors;

public class ResultAnalysis {
    private List<Map<String, Object>> nodes = new ArrayList<>();
    private List<Map<String, Object>> links = new ArrayList<>();
    private Set<Long> set = new HashSet<>();
    private Map<String, Object> retMap = new HashMap<>();
    private List<Map<String, Object>> retCategory = new ArrayList<>();

    public List<Map<String, Object>> getNodes() {
        return nodes;
    }

    public void setNodes(List<Map<String, Object>> nodes) {
        this.nodes = nodes;
    }

    public List<Map<String, Object>> getLinks() {
        return links;
    }

    public void setLinks(List<Map<String, Object>> links) {
        this.links = links;
    }

    public Set<Long> getSet() {
        return set;
    }

    public void setSet(Set<Long> set) {
        this.set = set;
    }

    public Map<String, Object> getRetMap() {
        return retMap;
    }

    public void setRetMap(Map<String, Object> retMap) {
        this.retMap = retMap;
    }

    public List<Map<String, Object>> getRetCategory() {
        return retCategory;
    }

    public void setRetCategory(List<Map<String, Object>> retCategory) {
        this.retCategory = retCategory;
    }

    public Map<String, Object> getResult(StatementResult result, List<Map<String, Object>> categories, Map<String, Object> param) {
        while (result.hasNext()) {
            Record record = result.next();
            analysisRecord(record,categories, param);
        }
        List<Map<String, Object>> linkWithoutDuplicate = links.stream().distinct().collect(Collectors.toList());
        List<Map<String, Object>> nodeWithoutDuplicate = nodes.stream().distinct().collect(Collectors.toList());
        Map<String, Object> data = new HashMap<>();
        ArrayList<Map<String, Object>> maps = new ArrayList<>();

        for(Map<String, Object> map:nodeWithoutDuplicate){
            Map<String, Object> camap = new HashMap<String,Object>();
            camap.put("name",map.get("nodeName"));
            maps.add(camap);
        }
//        data.put("categories", retCategory.stream().distinct().collect(Collectors.toList()));
        data.put("categories", maps);
        data.put("nodes", nodeWithoutDuplicate);
        data.put("links", linkWithoutDuplicate);
        retMap.put("data", data);
        return retMap;
    }

    public void analysisRecord(Record record, List<Map<String, Object>> categories,Map<String, Object> param) {
        analysisNode(record, categories,param);
        analysisRelationship(record);

    }

    public void analysisNode(Record record, List<Map<String, Object>> categories,Map<String, Object> param) {
        long m = record.get("m").asNode().id();
        long n = record.get("n").asNode().id();
        getNodes(m, "m", record, categories, param);
        getNodes(n, "n", record, categories, param);
    }

    public void getNodes(long nodeId, String item, Record record, List<Map<String, Object>> categories,Map<String, Object> param) {
        if (!set.contains(nodeId)) {
            Map<String, Object> node = new HashMap<>();
            node.put("name", nodeId + "");
            node.put("nodeName", record.get(item).asNode().get("name").asObject());
            node.put("key", record.get(item).asNode().get("id").asObject());
            node.put("belongto", record.get(item).asNode().get("belongto").asObject());
            int index = getCategory(record.get(item).asNode().labels(),param);
            node.put("category", index);
            node.put("colour", record.get(item).asNode().get("colour").asObject());
            node.put("grade", record.get(item).asNode().get("grade").asObject());
            Object grade = record.get(item).asNode().get("grade").asObject();
            if(Integer.parseInt(grade+"") >8){
                grade = 8;
            }

            node.put("picname", record.get(item).asNode().get("picname").asObject()+""+grade+".png");
            if(index != -1){
                nodes.add(node);
            }
            set.add(nodeId);
        }
    }

    public void analysisRelationship(Record record) {
        List<Object> Rel = record.get("r").asList();
        for (Object rel : Rel) {
            Map<String, Object> link = new HashMap<>();
            Relationship r = (Relationship) rel;
            link.put("source", r.startNodeId() + "");
            link.put("target", r.endNodeId() + "");
            link.put("rel", r.type());
            links.add(link);
        }
    }

    public Integer getCategory(Iterable<String> labels,Map<String, Object> param) {
        Integer index = 0;
        for (String label : labels) {
            if((label.contains("人口") || label.contains("时间") || label.contains("面积") || label.contains("制作人") || label.contains("确认人") || label.contains("预处理人") || label.contains("灾害影响") || label.contains("审批人") || label.contains("联系电话")) && (param.get("disaster") != null && !"".equals(param.get("disaster")))){
                return -1;
            }
            int flag = 0;
            for (int i = 0; i < retCategory.size(); i++) {
                if (label.equals(retCategory.get(i).get("name"))) {
                   index = i;
                   flag = 1;
                   break;
                }
            }
            if(flag == 0){
                Map<String, Object> temp = new HashMap<>();
                temp.put("name", label);
                retCategory.add(temp);
                index = retCategory.size() - 1;
            }
        }
        return index;
    }

}
