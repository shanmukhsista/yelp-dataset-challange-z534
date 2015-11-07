import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import dao.DBContext;

import java.net.UnknownHostException;

/**
 * Created by shanmukh on 11/6/15.
 */
public class Main {
    public static void main(String[] args) throws UnknownHostException {
        //Connect to the database.
        DBContext context = DBContext.getDatabaseContext();
        //Get the database object.
        DB db = context.getDataBase();
        DBCollection coll = context.getCollection("user");
        DBCursor cursor = coll.find();
        while (cursor.hasNext()) {
            DBObject currentObj = cursor.next();
            System.out.println(currentObj.get("votes"));
        }

    }
}
