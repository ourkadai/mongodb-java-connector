package com.dataworkz.mongo;

import com.mongodb.*;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Main {

    /**
     * Get From Local
     * @param databaseName
     * @return
     */
    private final static MongoDatabase getMongoDatabase(final String databaseName) {
        MongoClient mongo = new MongoClient("localhost", 27017);
        return mongo.getDatabase(databaseName);
    }

    private final static void cleanUp(final MongoDatabase database) {
        database.listCollectionNames().forEach((Consumer<? super String>) s -> {
                    database.getCollection(s).drop();
                }
        );
    }

    private final static void createCollection(final MongoDatabase database,
                                               final String collectionName) {
        database.createCollection(collectionName);
        Document document = new Document();
        document.put("my-key", "my-value");
        database.getCollection(collectionName).insertOne(document);
    }

    private final static List<String> getFieldNames(final MongoDatabase database,
                                                    final String collectionName) {

        MongoCollection coll = database.getCollection(collectionName);
        String mapFn = """
                function() {
                    for (var key in this) { emit(key, null); }
                  }
                """;
        String reduceFn = """
                function(key, stuff) { return null; }
                """;

        MapReduceIterable cars = coll.mapReduce(mapFn,reduceFn);
        List<String> strings = new ArrayList<>();
        cars.forEach((Block) o -> {
            strings.add(((Document) o).getString("_id"));
        });
        return strings;
    }

    public static void main(String[] args) {

        final MongoDatabase database = getMongoDatabase("my-database");

        cleanUp(database);

        createCollection(database,"my-collection");

        getFieldNames(database,"my-collection")
                .stream()
                .forEach(System.out::println);

    }
}
