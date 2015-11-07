package dao;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

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

    public DBCollection getCollection(String collectionName) {
        //Gets the collection object for the specified name
        return db.getCollection(collectionName);
    }
}
