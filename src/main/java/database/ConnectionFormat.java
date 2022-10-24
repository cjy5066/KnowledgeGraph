package database;

import java.net.URLEncoder;
import java.util.Map;

public class ConnectionFormat {
    /**
     * @Description 获取Neo4j APOC jdbc连接字符串
     * @Param [DBdata]
     * @Return java.lang.String
     * @Author zhangyunchao
     * @Date 2020/8/2
     * @Time 19:40
     */
    public static String getConnectionUrl(Map<String, Object> DBdata) throws Exception {
        DBdata.put("PASSWORD", URLEncoder.encode(String.valueOf(DBdata.get("PASSWORD"))));
        String connectionUrl = "";
        switch (DBdata.get("DATABASETYPE").toString()) {
            case "Oracle":
                    connectionUrl = "jdbc:oracle:" + "thin:" + DBdata.get("USERNAME") + "/" + DBdata.get("PASSWORD") + "@" + DBdata.get("IP")
                        + ":" + DBdata.get("PORT") + ":" + DBdata.get("DATABASENAME");
                break;
            case "MySQL":
                connectionUrl = "jdbc:mysql://" + DBdata.get("IP") + ":" + DBdata.get("PORT") + "/" + DBdata.get("DATABASENAME")
                        + "?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8&connectTimeout=250000&socketTimeout=300000&zeroDateTimeBehavior=round&user=" + DBdata.get("USERNAME")
                        + "&password=" + DBdata.get("PASSWORD");
                break;
            case "SqlServer":
                connectionUrl = "jdbc:sqlserver://" + DBdata.get("IP") + ":" + DBdata.get("PORT") + ";databaseName=" + DBdata.get("DATABASENAME")
                        + ";username=" + DBdata.get("USERNAME") + ";password=" + DBdata.get("PASSWORD");
                break;
            case "Postgres":
                connectionUrl = "jdbc:postgresql://" + DBdata.get("IP") + ":" + DBdata.get("PORT") + "/" + DBdata.get("DATABASENAME");
                break;
            case "Xugu":
                connectionUrl = "jdbc:xugu://" + DBdata.get("IP") + ":" + DBdata.get("PORT") + "/" + DBdata.get("DATABASENAME");
                break;
        }
        return connectionUrl;
    }
}
