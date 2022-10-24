package neo4j.query;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import java.util.*;
import java.util.stream.Collectors;

public class DijkstraAnalysis {
    private Map<String, Object> retMap = new HashMap<>();
    private List<Map<String, Object>> retCategory = new ArrayList<>();

    public Map<String, Object> getResult(StatementResult result, List<Map<String, Object>> categories, int flag) {
        List<Map<String, Object>> list = new ArrayList<>();
        while (result.hasNext()) {
            Record record = result.next();
            Map<String, Object> data = new HashMap<>();
            StringBuffer path = new StringBuffer();
            Integer distance = 0;
            data.put("nodes", getNodes(record,categories, path, distance, flag));
            data.put("links", getLinks(record, flag));
            data.put("distance", path.toString().split(",").length -1 );
            data.put("path", path.toString().substring(0,path.toString().lastIndexOf(",")));
            list.add(data);
        }
        retMap.put("data", list);
        retMap.put("categories", retCategory.stream().distinct().collect(Collectors.toList()));
//        retMap.put("categories", categories);
        return retMap;
    }

    public  List<Map<String, Object>> getNodes(Record record, List<Map<String, Object>> categories, StringBuffer path, Integer distance, int flag){
        List<Map<String, Object>> nodes = new ArrayList<>();
        java.util.Iterator<Node> iterator1 = record.get("p1").asPath().nodes().iterator();
        addNode(iterator1,categories,path,distance,nodes);
        if(flag == 1){
            java.util.Iterator<Node> iterator2 = record.get("p2").asPath().nodes().iterator();
            addNode(iterator2,categories,path,distance,nodes);
        }
        return nodes.stream().distinct().collect(Collectors.toList());
    }

    public void addNode(Iterator<Node> iterator, List<Map<String, Object>> categories, StringBuffer path, Integer distance, List<Map<String, Object>> nodes){
        while (iterator.hasNext()){
            Node n = iterator.next();
            Map<String, Object> node = new HashMap<>();
            node.put("name", n.id()+"");
            node.put("nodeName", n.asMap().get("name"));
            node.put("category", getCategory(n.labels()));
            nodes.add(node);
            path.append(n.asMap().get("name")) .append(",");
            distance++;
        }
    }

    public List<Map<String, Object>> getLinks(Record record, int flag){
        List<Map<String, Object>> links = new ArrayList<>();
        java.util.Iterator<Relationship> iterator1 = record.get("p1").asPath().relationships().iterator();
        addLink(iterator1, links);
        if(flag == 1){
            java.util.Iterator<Relationship> iterator2 = record.get("p2").asPath().relationships().iterator();
            addLink(iterator2, links);
        }
        return links.stream().distinct().collect(Collectors.toList());
    }

    public void addLink(Iterator<Relationship> iterator, List<Map<String, Object>> links){
        while (iterator.hasNext()){
            Relationship r = iterator.next();
            Map<String, Object> link = new HashMap<>();
            link.put("source", r.startNodeId() + "");
            link.put("target", r.endNodeId() + "");
            link.put("rel", r.type());
            links.add(link);
        }
    }

    public Integer getCategory(Iterable<String> labels) {
        Integer index = 0;
        for (String label : labels) {
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
