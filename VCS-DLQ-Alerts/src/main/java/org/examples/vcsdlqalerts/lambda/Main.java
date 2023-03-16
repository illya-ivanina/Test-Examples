package org.examples.vcsdlqalerts.lambda;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;

public class Main {
    static String avroSchemaName = "User";

    public static void main(String[] args) throws Exception {
        System.out.println("Starting...");
        var avroSchemaFile = Util.loadResourceFile("avroScheme.json").toPath();
        var avroSchema = Files.readString(avroSchemaFile);


        var convertor = new AvroToJsonConverter();

        //convertor.createAvroFile(avroSchema);
        //convertor.registerSchema(avroSchemaName, avroSchema);
        var json = convertor.avroToJson(avroSchemaName);
        System.out.println(json);
    }
}