package main.java;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import main.java.indexing.GenerateLuceneIndex;
import main.java.models.Tables;

import java.net.UnknownHostException;

/**
 * Created by shanmukh on 11/6/15.
 */

public class Main {
    public static void main(String[] args) throws UnknownHostException {
        System.out.println("===Indexing==");
        GenerateLuceneIndex gen = new GenerateLuceneIndex(Tables.REVIEW);
        gen = new GenerateLuceneIndex(Tables.TIP);

    }
}
