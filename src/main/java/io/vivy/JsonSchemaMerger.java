package io.vivy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import me.andrz.jackson.JsonReferenceProcessor;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class JsonSchemaMerger {

    private static ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static File mergeSchemasToFile(Path targetDir, Path... schemasDir) {
        return mergeSchemasToPath(targetDir, schemasDir).toFile();
    }

    public static Path mergeSchemasToPath(Path targetDir, Path... schemasDir) {
        try {
            Path schemaFile = Files.createTempFile(targetDir, "schema", ".json");

            return Files.write(schemaFile, mergeSchemas(schemasDir));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] mergeSchemas(Path... schemasDirectories) {
        try {
            JsonReferenceProcessor jsonReferenceProcessor = new JsonReferenceProcessor();

            ObjectNode schemaNode = JSON_MAPPER.createObjectNode();
            ObjectNode eventsNode = schemaNode.with("events");

            for (Path baseDir : schemasDirectories) {
                try (Stream<Path> pathsStream = Files.walk(baseDir)) {
                    pathsStream
                            .filter(Files::isRegularFile)
                            .filter(it -> !it.getFileName().toString().startsWith("_")) // Skip defs
                            .forEach(schemaFile -> {
                                try {
                                    ObjectNode eventNode = (ObjectNode) jsonReferenceProcessor.process(schemaFile.toFile());

                                    // Defaults
                                    ObjectNode propertiesNode = eventNode.with("properties");
                                    propertiesNode.with("eventId").put("type", "string");
                                    propertiesNode.with("eventType").put("type", "string");

                                    eventsNode.set(asType(baseDir, schemaFile), eventNode);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
                }
            }

            return JSON_MAPPER.writeValueAsBytes(schemaNode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String asType(Path parent, Path filePath) {
        String relativePath = parent.relativize(filePath).toString();
        return relativePath.substring(0, relativePath.lastIndexOf("."));
    }
}
