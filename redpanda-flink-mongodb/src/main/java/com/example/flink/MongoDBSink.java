package com.example.flink;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBSink extends RichSinkFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSink.class);
    private transient MongoCollection<Document> collection;

    @Override
    public void open(Configuration parameters) {
        // Create the MongoDB client to establish connection
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Collections.singletonList(new ServerAddress("mongo", 27017))))
                .codecRegistry(createCodecRegistry())
                .build();

        com.mongodb.client.MongoClient mongoClient = MongoClients.create(settings);

        // Access the MongoDB database and collection
        // At this stage, if the Mongo DB and collection does not exist, they would get auto-created
        MongoDatabase database = mongoClient.getDatabase("finance");
        collection = database.getCollection("financial_transactions");
    }

    @Override
    public void invoke(String value, Context context) {
        // Consume the event from redpanda topic
        LOG.info("Consumed event : " + value);
        Document transactionDocument = Document.parse(value);
        LOG.info("transactionDocument is : "+ transactionDocument);
        // Optionally you can add fraud detection logic as an additional exercise task from your end here
        // ...

        // Insert into MongoDB collection
        collection.insertOne(transactionDocument);
    }

    @Override
    public void close() {
        // Clean up resources, if needed
    }

    private CodecRegistry createCodecRegistry() {
        // The method createCodecRegistry is a helper method that is used to create a CodecRegistry object for MongoDB.
        // In MongoDB, a CodecRegistry is responsible for encoding and decoding Java objects to
        // BSON (Binary JSON) format, which is the native format used by MongoDB to store and retrieve data.
        return CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build())
        );
    }
}