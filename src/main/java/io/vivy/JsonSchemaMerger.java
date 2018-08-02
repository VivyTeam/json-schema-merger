package io.vivy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import me.andrz.jackson.JsonReferenceProcessor;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class JsonSchemaMerger {

    private static ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static File mergeSchemasToFile(Path schemasDir) {
        return mergeSchemasToPath(schemasDir).toFile();
    }

    public static Path mergeSchemasToPath(Path schemasDir) {
        try {
            Path schemaFile = Files.createTempFile("schema", ".json");

            return Files.write(schemaFile, mergeSchemas(schemasDir));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] mergeSchemas(Path schemasDir) {
        try {
            JsonReferenceProcessor jsonReferenceProcessor = new JsonReferenceProcessor();

            ObjectNode schemaNode = JSON_MAPPER.createObjectNode();
            ObjectNode eventsNode = schemaNode.with("events");
            Files.walk(schemasDir)
                    .filter(Files::isRegularFile)
                    .filter(it -> !it.getFileName().toString().startsWith("_")) // Skip defs
                    .forEach(it -> {
                        try {
                            String filePath = schemasDir.relativize(it).toString();
                            ObjectNode eventNode = (ObjectNode) jsonReferenceProcessor.process(it.toFile());

                            // Defaults
                            ObjectNode propertiesNode = eventNode.with("properties");
                            propertiesNode.with("eventId").put("type", "string");
                            propertiesNode.with("eventType").put("type", "string");

                            eventsNode.set(filePath.substring(0, filePath.lastIndexOf(".")), eventNode);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

            return JSON_MAPPER.writeValueAsBytes(schemaNode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
