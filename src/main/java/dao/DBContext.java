package main.java.dao;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
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
}
