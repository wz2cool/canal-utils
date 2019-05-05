package com.github.wz2cool.canal.utils.helper;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

public class ConfigHelper {
    /// region singleton
    private ConfigHelper() {
    }

    private static final ConfigHelper instance = new ConfigHelper();

    public static ConfigHelper getInstance() {
        return instance;
    }

    /// endregion


    public DatabaseInfo getDatabaseInfo() {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(Objects.requireNonNull(classLoader.getResource("database.properties")).getFile());
            try (InputStream inputStream = new FileInputStream(file)) {
                Properties prop = new Properties();
                prop.load(inputStream);
                String databaseTypePlaceholder = prop.getProperty("database-type");
                String databaseType;
                if (databaseTypePlaceholder.startsWith("${")) {
                    databaseType = System.getenv(databaseTypePlaceholder
                            .replace("${", "")
                            .replace("}", ""));
                } else {
                    databaseType = databaseTypePlaceholder;
                }

                databaseType = StringUtils.isBlank(databaseType) ? "h2" : databaseType;
                String dbPropertyFilePath = String.format("database-%s.properties", databaseType);
                File dbPropertyFile = new File(Objects.requireNonNull(classLoader.getResource(dbPropertyFilePath)).getFile());
                try (InputStream dbInputStream = new FileInputStream(dbPropertyFile)) {
                    Properties dbProperties = new Properties();
                    dbProperties.load(dbInputStream);
                    String className = dbProperties.getProperty("class-name");
                    String jdbcUrl = dbProperties.getProperty("jdbc-url");
                    String username = dbProperties.getProperty("username");
                    String password = dbProperties.getProperty("password");

                    DatabaseInfo result = new DatabaseInfo();
                    result.setDatabaseType(databaseType);
                    result.setClassName(className);
                    result.setJdbcUrl(jdbcUrl);
                    result.setUsername(username);
                    result.setPassword(password);
                    return result;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
