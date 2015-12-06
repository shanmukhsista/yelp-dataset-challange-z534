package main.java.indexing;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import main.java.dao.DBContext;
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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by vivek on 11/10/15.
 */
public class GenerateLuceneIndex {
    public void mongoGenerateLuceneIndex(Tables table_name) {
        try {
            System.out.println("Table being Indexed is '" + table_name + "'...");

            Directory dir = FSDirectory.open(Paths.get(System.getProperty("user.dir") + "_" + table_name));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

            IndexWriter writer = new IndexWriter(dir, iwc);

            //Connect to the database.
            DBContext context = DBContext.getDatabaseContext();
            //Get the database object.
            DB db = context.getDataBase();
            DBCollection coll = context.getCollection(table_name);
            List<String> distinctList = coll.distinct("business_id");
            Iterator<String> cursor = distinctList.iterator();

            HashMap<String, String> values = new HashMap<String, String>();

            switch (table_name) {

                case BUSINESS:
                    while (cursor.hasNext()) {
                        String currentBusiness_id = cursor.next();
                        System.out.println(currentBusiness_id);
                        //indexDoc()
                    }
                    break;
                case CHECKIN:
                    while (cursor.hasNext()) {
                        String currentBusiness_id = cursor.next();
                        System.out.println(currentBusiness_id);
                        //indexDoc()
                    }
                    break;
                case REVIEW:
                    float count = 0;
                    while (cursor.hasNext()) {
                        String currentBusiness_id = cursor.next();
                        //System.out.println(currentObj.get("business_id") + "->" +  currentObj.get("text"));
                        values.put("business_id", currentBusiness_id);
                        System.out.println(++count);
                        //Get All Reviews
                        DBCursor reviewcursor = coll.find(context.getReviewForCollection(Tables.REVIEW, currentBusiness_id));
                        while (reviewcursor.hasNext()) {
                            DBObject currentReviewObj = reviewcursor.next();
                            if (values.containsKey("text")) {
                                values.put("text", values.get("text") + currentReviewObj.get("text").toString());
                            } else {
                                values.put("text", currentReviewObj.get("text").toString());
                            }
                        }

                        indexDoc(writer, values);
                        values.clear();
                    }
                    break;
                case TIP:
                    while (cursor.hasNext()) {
                        String currentBusiness_id = cursor.next();
                        //System.out.println(currentObj.get("business_id") + "->" +  currentObj.get("text"));
                        values.put("business_id", currentBusiness_id);
                        //Get All Reviews
                        DBCursor reviewcursor = coll.find(context.getReviewForCollection(Tables.TIP, currentBusiness_id));
                        while (reviewcursor.hasNext()) {
                            DBObject currentReviewObj = reviewcursor.next();
                            if (values.containsKey("text")) {
                                values.put("text", values.get("text") + currentReviewObj.get("text").toString());
                            } else {
                                values.put("text", currentReviewObj.get("text").toString());
                            }
                        }

                        indexDoc(writer, values);
                        values.clear();
                    }
                    break;
                case USER:
                    while (cursor.hasNext()) {
                        String currentBusiness_id = cursor.next();
                        System.out.println(currentBusiness_id);
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

    public void RawGenerateLuceneIndex(Tables table_name) {
        try {
            System.out.println("Table being Indexed is '" + table_name + "'...");

            //Lucene Index Init
            Directory dir = FSDirectory.open(Paths.get(System.getProperty("user.dir") + "_" + table_name));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            IndexWriter writer = new IndexWriter(dir, iwc);

            //MongoDB Init
            //Connect to the database.
            DBContext context = DBContext.getDatabaseContext();
            //Get the database object.
            DBCollection coll = context.getCollection(table_name);

            DBCursor cursor = coll.find();

            HashMap<String, String> values = new HashMap<String, String>();
            int count = 0;
            while (cursor.hasNext()) {
                DBObject currObj = cursor.next();
                values.put("business_id", currObj.get("business_id").toString());
                values.put("text", currObj.get("text").toString());
                indexDoc(writer, values);
                System.out.println(++count);
                values.clear();
            }


            writer.forceMerge(1);
            writer.commit();
            writer.close();
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass()
                    + "\n with message: " + e.getMessage());
        }
    }

    void indexDoc(IndexWriter writer, HashMap<String, String> values) throws IOException {
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

    public void GenerateTrainingLuceneIndex(Tables tablename) {

        //Lucene Index Init
        Directory dir = null;
        try {
            dir = FSDirectory.open(Paths.get(System.getProperty("user.dir") + "_" + tablename + "FINALTRAINING"));

            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            IndexWriter writer = new IndexWriter(dir, iwc);

            SearchIndex s = new SearchIndex();
            DBContext context = DBContext.getDatabaseContext();

            //Get the database object.
            DB db = context.getDataBase();
            DBCollection coll = context.getCollection(tablename);
            List<String> distinctList = coll.distinct("business_id");
            Iterator<String> cursor = distinctList.iterator();
            HashMap<String, String> values = new HashMap<String, String>();
            int count = 0;
            while (cursor.hasNext()) {
                System.out.println(++count);
                String currentBusinessID = cursor.next();
                if (count <= 42550) {   //Training
                    // if(count>42550) {   //Test


                    List<String> reviews = s.GetReviews(currentBusinessID, Tables.REVIEW.toString());
                    List<String> tips = s.GetReviews(currentBusinessID, Tables.TIP.toString());
                    values.put("business_id", currentBusinessID);
                    StringBuffer sb = new StringBuffer();

                    for (int i = 0; i < reviews.size(); i++) {
                        sb.append(reviews.get(i));
                    }

                    for (int i = 0; i < tips.size(); i++) {
                        sb.append(tips.get(i));
                    }
                    values.put("text", sb.toString());
                    indexDoc(writer, values);

                    values.clear();
                    sb.delete(0, sb.length());
                }
            }
            writer.forceMerge(1);
            writer.commit();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void GenerateTestLuceneIndex(Tables tablename) {

        //Lucene Index Init
        Directory dir = null;
        try {
            dir = FSDirectory.open(Paths.get(System.getProperty("user.dir") + "_" + tablename + "FINALTEST"));

            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            IndexWriter writer = new IndexWriter(dir, iwc);

            SearchIndex s = new SearchIndex();
            DBContext context = DBContext.getDatabaseContext();

            //Get the database object.
            DB db = context.getDataBase();
            DBCollection coll = context.getCollection(tablename);
            List<String> distinctList = coll.distinct("business_id");
            Iterator<String> cursor = distinctList.iterator();
            HashMap<String, String> values = new HashMap<String, String>();
            int count = 0;
            while (cursor.hasNext()) {
                System.out.println(++count);
                String currentBusinessID = cursor.next();
                //if(count<=42550) {   //Training
                if (count > 42550) {   //Test


                    List<String> reviews = s.GetReviews(currentBusinessID, Tables.REVIEW.toString());
                    List<String> tips = s.GetReviews(currentBusinessID, Tables.TIP.toString());
                    values.put("business_id", currentBusinessID);
                    StringBuffer sb = new StringBuffer();

                    for (int i = 0; i < reviews.size(); i++) {
                        sb.append(reviews.get(i));
                    }

                    for (int i = 0; i < tips.size(); i++) {
                        sb.append(tips.get(i));
                    }
                    values.put("text", sb.toString());
                    indexDoc(writer, values);

                    values.clear();
                    sb.delete(0, sb.length());
                }
            }
            writer.forceMerge(1);
            writer.commit();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
