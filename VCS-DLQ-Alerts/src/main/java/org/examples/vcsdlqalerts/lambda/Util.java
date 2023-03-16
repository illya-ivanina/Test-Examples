package org.examples.vcsdlqalerts.lambda;

import org.apache.avro.Schema;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Util {
    public static File loadResourceFile(String filename) throws URISyntaxException {
        var classLoader = AvroToJsonConverter.class.getClassLoader();
        var resourceUrl = classLoader.getResource(filename);
        return Paths.get(resourceUrl.toURI()).toFile();
    }

    public static Schema getSchemeFromString(String schemaString) {
        return new Schema.Parser().parse(schemaString);
    }

    public static String getSchemeStringFromFile() throws Exception {
        var avroSchemaFile = Util.loadResourceFile("avroScheme.json").toPath();
        return Files.readString(avroSchemaFile);
    }
}
