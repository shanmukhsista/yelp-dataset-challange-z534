package main.java.indexing;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import main.java.models.Tables;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import main.java.dao.DBContext;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by vivek on 11/10/15.
 */
public class GenerateLuceneIndex {
    public GenerateLuceneIndex(Tables table_name) {
        try {
            System.out.println("Table being Indexed is '" + table_name + "'...");

            Directory dir = FSDirectory.open(Paths.get(System.getProperty("user.dir") + "\\" + table_name + "\\"));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

            IndexWriter writer = new IndexWriter(dir, iwc);


            //Connect to the database.
            DBContext context = DBContext.getDatabaseContext();
            //Get the database object.
            DB db = context.getDataBase();
            DBCollection coll = context.getCollection(table_name);
            DBCursor cursor = coll.find();

            switch (table_name) {

                case BUSINESS:
                    while (cursor.hasNext()) {
                        DBObject currentObj = cursor.next();
                        System.out.println(currentObj.get("business_id"));
                        //indexDoc()
                    }
                    break;
                case CHECKIN:
                    while (cursor.hasNext()) {
                        DBObject currentObj = cursor.next();
                        System.out.println(currentObj.get("business_id"));
                        //indexDoc()
                    }
                    break;
                case REVIEW:
                    while (cursor.hasNext()) {
                        DBObject currentObj = cursor.next();
                        System.out.println(currentObj.get("business_id") + "->" + currentObj.get("user_id") + "->" + currentObj.get("text"));
                    }
                    break;
                case TIP:
                    while (cursor.hasNext()) {
                        DBObject currentObj = cursor.next();
                        System.out.println(currentObj.get("business_id") + "->" + currentObj.get("user_id") + "->" + currentObj.get("text"));
                        //indexDoc()
                    }
                    break;
                case USER:
                    while (cursor.hasNext()) {
                        DBObject currentObj = cursor.next();
                        System.out.println(currentObj.get("review_count") + "->" + currentObj.get("user_id") + "->" + currentObj.get("average_stars"));
                        //indexDoc()
                    }
                    break;

            }

            writer.forceMerge(1);
            writer.commit();
            writer.close();
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass()
                    + "\n with message: " + e.getMessage());
        }
    }

    static void indexDoc(IndexWriter writer, String key, Map<String, String> values) throws IOException {
        // make a new, empty document

        if (values.containsKey("business_id")) {
            Document lDoc = new Document();

            lDoc.add(new StringField("business_id", values.get("business_id"), Field.Store.YES));
            values.remove("business_id");

            Iterator it = values.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                lDoc.add(new TextField(pair.getKey().toString(), pair.getValue().toString(), Field.Store.YES));
                it.remove(); // avoids a ConcurrentModificationException
            }

            writer.addDocument(lDoc);
        }
    }
}
