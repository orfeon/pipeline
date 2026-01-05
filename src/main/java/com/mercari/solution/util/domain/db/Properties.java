package com.mercari.solution.util.domain.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Properties implements Serializable {

    // seek
    public String table;
    public String select;
    public List<String> seekFields;
    public Integer fetchSize;

    // split
    public Boolean enableSplit;
    public CharCollation charCollation;

    // jdbc driver
    public String driver;
    public String url;
    public String user;
    public String password;
    public Map<String, String> dataSourceProperties;

    public Properties() {

    }

    public Properties(
            final String table,
            final String select,
            final List<String> seekFields,
            final Integer fetchSize,
            final Boolean enableSplit,
            final String driver,
            final String url,
            final String user,
            final String password,
            final CharCollation charCollation,
            final Map<String, String> dataSourceProperties) {

        this.table = table;
        this.select = Optional
                .ofNullable(select)
                .orElse("*");
        this.seekFields = seekFields;
        this.fetchSize = Optional
                .ofNullable(fetchSize)
                .orElse(10000);
        this.enableSplit = enableSplit;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
        this.charCollation = charCollation;
        this.dataSourceProperties = dataSourceProperties;
    }

    public static class Boundary implements Serializable {

        public String name;

    }

    public static HikariDataSource createDataSource(
            final Properties properties) {

        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.url);
        if(properties.driver != null) {
            config.setDriverClassName(properties.driver);
        }
        if(properties.user != null) {
            config.setUsername(properties.user);
        }
        if(properties.password != null) {
            config.setPassword(properties.password);
        }

        if(properties.dataSourceProperties != null) {
            for(final Map.Entry<String, String> entry : properties.dataSourceProperties.entrySet()) {
                config.addDataSourceProperty(entry.getKey(), entry.getValue());
            }
        }

        config.setReadOnly(true);
        config.setAutoCommit(false);
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    public static HikariDataSource createDataSource(
            final String url,
            final String user,
            final String password,
            final Map<String, String> dataSourceProperties) {

        final Properties properties = new Properties();
        properties.url = url;
        properties.user = user;
        properties.password = password;
        properties.dataSourceProperties = dataSourceProperties;
        return createDataSource(properties);
    }

}
