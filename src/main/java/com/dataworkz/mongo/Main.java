package com.dataworkz.mongo;

import com.mongodb.*;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    /**
     * Get From Atlas
     * @param databaseName
     * @return
     */
    private final static MongoDatabase getMongoDatabase(final String connectionString,
                                                        final String databaseName) {
        com.mongodb.client.MongoClient mongo = MongoClients.create(connectionString);
        return mongo.getDatabase(databaseName);
    }

    /**
     * Get From Local
     * @param databaseName
     * @return
     */
    private final static MongoDatabase getMongoDatabase(final String host,
                                                        final int port,
                                                        final String databaseName) {
        MongoClient mongo = new MongoClient(host, port);
        return mongo.getDatabase(databaseName);
    }

    private final static Map<String,Collection<String>> getMetadata(final MongoDatabase database) {
        Map<String,Collection<String>> metadata = new HashMap<>();
        database.listCollectionNames().forEach((Consumer<? super String>) collectionName -> {
            metadata.put(collectionName, getFieldNamesFailover(database,collectionName));
        });
        return metadata;
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
        List<String> keys = new ArrayList<>();
        cars.forEach((Block) o -> {
            keys.add(((Document) o).getString("_id"));
        });
        return keys;
    }

    private final static Set<String> getFieldNamesFailover(final MongoDatabase database,
                                                           final String collectionName) {

        Set<String> keys = new TreeSet<>();
        MongoCollection<Document> coll = database.getCollection(collectionName);

        coll.find().forEach((Consumer) o -> {
            ((Document) o).keySet().forEach(s -> keys.add(s));
        });

        return keys;
    }

    public static void main(String[] args) {

        Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);

//      Fetch data from local
        final MongoDatabase database = getMongoDatabase("localhost",27017,
                "food");
//      Fetch data from cloud
//      final MongoDatabase database = getMongoDatabase(CONNECTION_STRING,
//                "DataworkzDatabase");
        getMetadata(database).entrySet()
                .stream()
                .forEach(System.out::println);
    }
}
