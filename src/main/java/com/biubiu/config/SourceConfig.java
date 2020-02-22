package com.biubiu.config;

public class SourceConfig {
    private String url;
    private String username;
    private String catalog;
    private String schema;
    private String table;

    public SourceConfig() {
    }

    public SourceConfig(String url, String username, String catalog, String schema, String table) {
        this.url = url;
        this.username = username;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
