package org.examples.vcsdlqalerts.lambda;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class Config {
    private static final String ENV_VAR_NAME = "MY_PROPERTY";
    private static final String DEFAULT_CONFIG_FILE = "app.properties";

    private static final Properties properties = new Properties();

    static {
        try (InputStream input = Config.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE)) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        for (var key : properties.keySet()) {
            var _key = String.valueOf(key);
            String envVarValue = System.getenv(_key);
            if (envVarValue != null && !envVarValue.trim().isEmpty()) {
                properties.setProperty(_key, envVarValue);
            }
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
