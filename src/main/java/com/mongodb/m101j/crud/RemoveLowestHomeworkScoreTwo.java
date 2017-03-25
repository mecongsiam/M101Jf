package com.mongodb.m101j.crud;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by meco on 17.03.2017.
 */
public class RemoveLowestHomeworkScoreTwo {

    public static void main(String[] args) {
        try {
            Runtime.getRuntime()
                    .exec("mongoimport --drop -d school -c students " +
                            "D:\\mongo_st\\mongoWeekCrud\\src\\main\\resources\\students.json");
        } catch (IOException e) {
            e.printStackTrace();
        }
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("school");
        final MongoCollection<Document> collection = database.getCollection("students");
        AggregateIterable<Document> minHomeWorkScore = collection.aggregate(Arrays.asList(new Document("$unwind", "$scores"),
                new Document("$match", new Document("scores.type", "homework")),
                new Document("$group", new Document("_id", "$_id")
                        .append("score", new Document("$min", "$scores.score"))
                        .append("type", new Document("$first", "$scores.type")))));

        for (Document document : minHomeWorkScore) {
            collection.updateOne(new Document("_id", document.get("_id")),
                    new Document("$pull", new Document("scores", new Document("type", document.get("type"))
                            .append("score", document.get("score")))));
        }
        client.close();
    }
}
