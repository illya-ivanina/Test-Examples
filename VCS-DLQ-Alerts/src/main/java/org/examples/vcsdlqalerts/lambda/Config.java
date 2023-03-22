package org.examples.vcsdlqalerts.lambda;

import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

public class Config {
    private static final Logger log = LoggerFactory.getLogger(Config.class);
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
            var envVarValue = getFromEnvAndDecryptVariable(_key);
            if (envVarValue != null && !envVarValue.trim().isEmpty()) {
                properties.setProperty(_key, envVarValue);
            }
        }
    }

    public static String getProperty(String key) {
        var value = properties.getProperty(key);
        if (value != null && !value.trim().isEmpty()) {
            return getFromEnvAndDecryptVariable(key);
        }
        return value;
    }

    private static String getFromEnvAndDecryptVariable(String envName) {
        var value = System.getenv(envName);
        if (System.getenv("PROFILE") != null && System.getenv("PROFILE").equals("local")) {
            return value;
        }
        if (value == null || value.trim().isEmpty()) {
            return value;
        }
        byte[] encryptedKey = Base64.decode(System.getenv(envName));
        var encryptionContext = Map.of(
                "LambdaFunctionName",
                System.getenv("AWS_LAMBDA_FUNCTION_NAME"));
        try {
            var clientAWSKMS = AWSKMSClientBuilder.defaultClient();
            var decryptRequest = new DecryptRequest()
                    .withCiphertextBlob(ByteBuffer.wrap(encryptedKey))
                    .withEncryptionContext(encryptionContext);
            var byteBufferPlainTextKey = clientAWSKMS.decrypt(decryptRequest).getPlaintext();
            return new String(byteBufferPlainTextKey.array(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("Error decrypting variable: " + envName, e);
            return System.getenv(envName);
        }
    }
}
