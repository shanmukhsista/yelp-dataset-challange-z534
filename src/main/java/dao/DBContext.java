package main.java.dao;

import com.mongodb.*;
import main.java.models.Tables;

import java.net.UnknownHostException;

/**
 * Created by shanmukh on 11/6/15.
 */
public class DBContext {
    private static DBContext context = new DBContext();
    private MongoClient mongo;
    private DB db;

    private DBContext() {
        try {
            mongo = new MongoClient("localhost", 27017);
            db = mongo.getDB("yelp_challange");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DBContext getDatabaseContext() throws UnknownHostException {
        return context;
    }

    public DB getDataBase() {
        return this.db;
    }

    public DBCollection getCollection(Tables collectionName) {
        //Gets the collection object for the specified name
        switch (collectionName) {

            case BUSINESS:
                return db.getCollection("business");
            case CHECKIN:
                return db.getCollection("checkin");
            case REVIEW:
                return db.getCollection("review");
            case TIP:
                return db.getCollection("tip");
            case USER:
                return db.getCollection("user");

        }

        return null;
    }

    public BasicDBObject getReviewForCollection(Tables collectionName, String key) {
        //Gets the collection object for the specified name
        BasicDBObject whereQuery = new BasicDBObject();
        switch (collectionName) {

            case BUSINESS:
                whereQuery.put("business_id", key);
                return whereQuery;
            case CHECKIN:
                whereQuery.put("business_id", key);
                return whereQuery;
            case REVIEW:
                whereQuery.put("business_id", key);
                return whereQuery;
            case TIP:
                whereQuery.put("business_id", key);
                return whereQuery;
            case USER:
                whereQuery.put("business_id", key);
                return whereQuery;
        }

        return null;
    }
}
