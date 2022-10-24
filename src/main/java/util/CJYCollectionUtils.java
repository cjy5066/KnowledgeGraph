package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CJYCollectionUtils {

  /**
   * 用指定的k字段获取list中的值，用作于map中k建
   *
   * @param listdata  要转换的list
   * @param list_K	指定的k字段，获取list中的值用作于map中k键
   * @param list_V	指定的v字段，获取list中的值用作于map中v值
   * @return	list转map
   */
  public static Map<String,Object> listTomap(List<Map<String,Object>> listdata,String list_K,String list_V){
    Map<String,Object> maps = new HashMap<String,Object>();

    for(Map<String,Object> map : listdata){
      String k = String.valueOf(map.get(list_K));
      Object v = map.get(list_V);

      maps.put(k, v);
    }

    return maps;
  }




  /**
   * 用指定的k字段获取list中map
   *
   * @param listdata  要转换的list
   * @param list_K	指定的k字段，管理list中的map
   * @return	list转map
   */
  public static Map<String,Map<String,Object>> listToIdGetMap(List<Map<String,Object>> listdata,String list_K){
    Map<String,Map<String,Object>> maps = new HashMap<String,Map<String,Object>>();

    for(Map<String,Object> map : listdata){
      String k = String.valueOf(map.get(list_K));
      maps.put(k, map);
    }

    return maps;
  }


  /**
   * 用指定的k 去管理list 【分组】
   * @param mapList
   * @param key
   * @return
   */
  public static Map<String, List<Map<String, Object>>> groupListMap(List<Map<String, Object>> mapList, String key) {
    Map<String, List<Map<String, Object>>> classificationMap = new HashMap<String, List<Map<String, Object>>>();
    for (Map<String,Object> dataMap : mapList) {
      String phenomenonKey = dataMap.get(key)+"";
      List<Map<String, Object>> mList = classificationMap.get(phenomenonKey);
      if (mList == null) {
        mList = new ArrayList<Map<String, Object>>();
        classificationMap.put(phenomenonKey, mList);
      }
      mList.add(dataMap);
    }
    return classificationMap;
  }


}
