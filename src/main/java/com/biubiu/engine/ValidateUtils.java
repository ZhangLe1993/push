package com.biubiu.engine;

import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ValidateUtils {

    public final static List<String> SOURCE_FIELD_COLUMNS = Arrays.asList(
            "prestoUrl",   		            // url
            "prestoUser",                   // username
            "prestoCatalog",      	        // catalog
            "prestoSchema",			        // schema
            "prestoTable"			        // table
    );

    public final static List<String> TARGET_FIELD_COLUMNS = Arrays.asList(
            "mysqlUrl",
            "mysqlHost",
            "mysqlPort",
            "mysqlDb",
            "mysqlUser",
            "mysqlPassword",
            "table"
    );

    public final static List<String> META_INFO_FIELD_COLUMNS = Arrays.asList(
            "pgsqlUrl",
            "pgsqlHost",
            "pgsqlPort",
            "pgsqlUser",
            "pgsqlPassword",
            "pgsqlDb",
            "pgsqlSchema",
            "pgsqlTable",
            "mysqlHost",
            "mysqlPort",
            "mysqlDb",
            "mysqlUser",
            "mysqlPassword",
            "table",
            "mysqlUrl"
    );


    public static boolean validateColumns(JSONObject o, List<String> mustFieldNames) {
        if(!Objects.isNull(o)) {
            boolean b = mustFieldNames.stream().allMatch(c -> o.containsKey(c) && Optional.ofNullable(o.get(c)).isPresent());
            if(!b) return false;
            return true;
        }
        return false;
    }
}
