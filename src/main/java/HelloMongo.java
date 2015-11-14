/**
 * Created by sbode on 14.11.15.
 */
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;

public class HelloMongo {

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("test");

        for (String database : mongoClient.listDatabaseNames()) {
            System.out.println("Database: " + database);
        }

        for (String collectionName : db.listCollectionNames()) {
            System.out.println("Collection: " + collectionName);
        }

        db.getCollection("restaurants").createIndex(new Document("cuisine", 1));

        for (final Document index : db.getCollection("restaurants").listIndexes()) {
            System.out.println(index.toJson());
        }

        System.out.println(db.runCommand(new Document("buildInfo", 1)));

        db.getCollection("restaurants").insertOne(getDocument());

//        FindIterable<Document> iterable = db.getCollection("restaurants").find(eq("borough", "Manhattan"));
//        FindIterable<Document> iterable = db.getCollection("restaurants").find(eq("address.zipcode", "10075"));
//        FindIterable<Document> iterable = db.getCollection("restaurants").find(eq("grades.grade", "B"));
//        FindIterable<Document> iterable = db.getCollection("restaurants").find(gt("grades.score", 30));
//        FindIterable<Document> iterable = db.getCollection("restaurants").find(lt("grades.score", 10));
//        FindIterable<Document> iterable = db.getCollection("restaurants").find(or(eq("cuisine", "Italian"), eq("address.zipcode", "10075"))).sort(ascending("borough"));
        FindIterable<Document> iterable = db.getCollection("restaurants").find().sort(ascending("borough", "address.zipcode"));
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
//                System.out.println(document);
            }
        });

//        UpdateResult updateResult1 = db.getCollection("restaurants").updateOne(new Document("name", "Juni"),
//                new Document("$set", new Document("cuisine", "American (New)"))
//                        .append("$currentDate", new Document("lastModified", true)));
//        System.out.println(updateResult1.getModifiedCount() + " documents were updated by update 1");
//
//        UpdateResult updateResult2 = db.getCollection("restaurants").updateOne(new Document("restaurant_id", "41156888"),
//                new Document("$set", new Document("address.street", "East 31st Street")));
//        System.out.println(updateResult2.getModifiedCount() + " documents were updated by update 2");
//
//        UpdateResult updateResult3 = db.getCollection("restaurants").updateMany(new Document("address.zipcode", "10016").append("cuisine", "Other"),
//                new Document("$set", new Document("cuisine", "Category To Be Determined"))
//                        .append("$currentDate", new Document("lastModified", true)));
//        System.out.println(updateResult3.getModifiedCount() + " documents were updated by update 3");
//
//        UpdateResult updateResult4 = db.getCollection("restaurants").replaceOne(new Document("restaurant_id", "41704620"),
//                new Document("address",
//                        new Document()
//                                .append("street", "2 Avenue")
//                                .append("zipcode", "10075")
//                                .append("building", "1480")
//                                .append("coord", asList(-73.9557413, 40.7720266)))
//                        .append("name", "Vella 2"));
//        System.out.println(updateResult4.getModifiedCount() + " documents were updated by update 4");

        DeleteResult deleteResult = db.getCollection("restaurants").deleteOne(new Document("restaurant_id", "41704620"));
        System.out.println(deleteResult.getDeletedCount() + " documents were deleted");

        AggregateIterable<Document> aggregateIterable1 = db.getCollection("restaurants").aggregate(asList(
                new Document("$group", new Document("_id", "$borough").append("count", new Document("$sum", 1)))));
        aggregateIterable1.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        });

        AggregateIterable<Document> aggregateIterable2 = db.getCollection("restaurants").aggregate(asList(
                new Document("$match", new Document("borough", "Queens").append("cuisine", "Brazilian")),
                new Document("$group", new Document("_id", "$address.zipcode").append("count", new Document("$sum", 1)))));
        aggregateIterable2.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        });

        mongoClient.close();
    }

    private static Document getDocument() {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
        Document d = null;
        try {
            d = new Document("address",
                    new Document()
                            .append("street", "2 Avenue")
                            .append("zipcode", "10075")
                            .append("building", "1480")
                            .append("coord", asList(-73.9557413, 40.7720266)))
                    .append("borough", "Manhattan")
                    .append("cuisine", "Italian")
                    .append("grades", asList(
                            new Document()
                                    .append("date", format.parse("2014-10-01T00:00:00Z"))
                                    .append("grade", "A")
                                    .append("score", 11),
                            new Document()
                                    .append("date", format.parse("2014-01-16T00:00:00Z"))
                                    .append("grade", "B")
                                    .append("score", 17)))
                    .append("name", "Vella")
                    .append("restaurant_id", "41704620");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return d;
    }
}
