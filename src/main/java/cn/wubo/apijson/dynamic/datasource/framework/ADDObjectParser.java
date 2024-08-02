package cn.wubo.apijson.dynamic.datasource.framework;

import apijson.RequestMethod;
import apijson.framework.APIJSONObjectParser;
import apijson.framework.APIJSONSQLConfig;
import apijson.orm.Join;
import apijson.orm.SQLConfig;
import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpSession;
import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * ADDObjectParser
 * 重新数据源初始化
 *
 * @author 吴博
 * @version 1.0
 * @date 2022.09.05
 */
@Slf4j
public class ADDObjectParser extends APIJSONObjectParser {

    public ADDObjectParser(HttpSession session, JSONObject request, String parentPath, SQLConfig arrayConfig, boolean isSubquery, boolean isTable, boolean isArrayMainTable) throws Exception {
        super(session, request, parentPath, arrayConfig, isSubquery, isTable, isArrayMainTable);
    }

    @Override
    public SQLConfig newSQLConfig(RequestMethod method, String table, String alias, JSONObject request, List<Join> joinList, boolean isProcedure) throws Exception {
        ADDSQLConfig addSQLConfig = (ADDSQLConfig) APIJSONSQLConfig.newSQLConfig(method, table, alias, request, joinList, isProcedure);
        Map<String, Object> map = getCustomMap();

        String dsUrl = "";
        String dsUserName = "";
        String dsPassword = "";
        String dbType = "";

        if (map != null && map.size() > 0 && map.containsKey("@dsUrl") && map.containsKey("@dsUserName") && map.containsKey("@dsPassword") && map.containsKey("@dbType")) {
            dsUrl = String.valueOf(map.get("@dsUrl"));
            dsUserName = String.valueOf(map.get("@dsUserName"));
            dsPassword = String.valueOf(map.get("@dsPassword"));
            dbType = String.valueOf(map.get("@dbType"));
        } else {
            // 使用默认数据源
            DruidDataSource druidDataSource = (DruidDataSource) SpringUtil.getBean(DataSource.class);
            dsUrl = druidDataSource.getUrl();
            dsUserName = druidDataSource.getUsername();
            dsPassword = druidDataSource.getPassword();
            dbType = "mysql"; // 假设默认是MySQL
        }

        int i = dsUrl.lastIndexOf("/");
        int j = dsUrl.lastIndexOf("?");
        String start = dsUrl.substring(0, i);
        String end = j > 0 ? dsUrl.substring(j) : "";
        String db = dsUrl.substring(i + 1, j > 0 ? j : dsUrl.length());

        addSQLConfig.setDb(start + end, dsUserName, dsPassword, db, dbType);
        return addSQLConfig;
    }

}
