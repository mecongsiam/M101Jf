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
public class RemoveLowestHomeworkScoreOne {
    public static void main(String[] args) {
        try {
            Runtime.getRuntime()
                    .exec("mongoimport --drop -d students -c grades D:\\mongo_st\\mongoWeekCrud\\src\\main\\resources\\grades.json");
        } catch (IOException e) {
            e.printStackTrace();
        }
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("students");
        final MongoCollection<Document> collection = database.getCollection("grades");
        AggregateIterable<Document> minHomeworkScore = collection.aggregate(Arrays.asList(new Document("$match", new Document("type", "homework")),
                new Document("$group", new Document("_id", "$student_id")
                        .append("score", new Document("$min", "$score"))
                        .append("id", new Document("$first", "$_id"))
                        .append("type", new Document("$first", "$type")))
        ));
        for(Document document:minHomeworkScore){
            collection.deleteOne(new Document("_id",document.get("id")));
        }
        client.close();
    }

}
