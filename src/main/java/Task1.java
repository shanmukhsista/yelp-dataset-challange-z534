package main.java;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import main.java.dao.DBContext;
import main.java.indexing.GenerateLuceneIndex;
import main.java.indexing.ParseIndex;
import main.java.indexing.SearchIndex;
import main.java.indexing.StanfordCoreNlpDemo;
import main.java.models.Tables;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by shanmukh on 11/16/15.
 */
public class Task1 {
    public Task1() {

    }

    public static void run() {
        System.out.println("===Indexing==");
        GenerateLuceneIndex g = new GenerateLuceneIndex();
        // MongoDB based Indexing Slow
        g.mongoGenerateLuceneIndex(Tables.REVIEW);
        g.mongoGenerateLuceneIndex(Tables.TIP);

        // Lucene to Lucene Indexing Fast
        Tables tablename = Tables.REVIEW;



        //Step1 - Basic Lucene Index
        //g.RawGenerateLuceneIndex(tablename);

        ParseIndex p = new ParseIndex();

        //p.stats(tablename);

        //step2 - Businessid to Reviews+tips index
        //g.GenerateTrainingLuceneIndex(tablename);
        //g.GenerateTestLuceneIndex(tablename);

        ParseIndex p1 = new ParseIndex();

        //p.stats(tablename.toString());

        SearchIndex s = new SearchIndex();
        StanfordCoreNlpDemo nlp = new StanfordCoreNlpDemo();
        //System.out.println(nlp.StanfordDependency("dr. goldberg offers everything i look for in a general practitioner.  he's nice and easy to talk to without being patronizing; he's always on time in seeing his patients; he's affiliated with a top-notch hospital (nyu) which my parents have explained to me is very important in case something happens and you need surgery; and you can get referrals to see specialists without having to see him first.  really, what more do you need?  i'm sitting here trying to think of any complaints i have about him, but i'm really drawing a blank.")
        //);


        try {

            String index = System.getProperty("user.dir") + "_" + Tables.REVIEW + "FINALTEST";
            IndexReader reader = null;
            reader = DirectoryReader.open(FSDirectory.open(Paths
                    .get(index)));
            IndexSearcher searcher = new IndexSearcher(reader);
            // Get the segments of the index
            List<LeafReaderContext> leafContexts = reader.getContext().reader()
                    .leaves();

            //Connect to the database.
            DBContext context = DBContext.getDatabaseContext();
            //Get the database object.
            DB db = context.getDataBase();
            DBCollection coll = context.getCollection(Tables.BUSINESS);


            //Delimiter used in CSV file
            final String COMMA_DELIMITER = ",";
            final String NEW_LINE_SEPARATOR = "\n";
            final String FILE_HEADER = "businessid,truecategory,business1,categories,business2,categories,business3,categories,business4,categories,business5,categories,business6,categories,business7,categories";


            FileWriter fileWriter = null;
            fileWriter = new FileWriter(System.getProperty("user.home") + "/task1-1.csv");
            System.out.println(System.getProperty("user.home"));

            //Write the CSV file header
            fileWriter.append(FILE_HEADER.toString());

            //Add a new line separator after the header
            fileWriter.append(NEW_LINE_SEPARATOR);


            // Processing each segment
            for (int i = 0; i < leafContexts.size(); i++) {  //leafContexts.size()
                // Get document length
                LeafReaderContext leafContext = leafContexts.get(i);
                int startDocNo = leafContext.docBase;
                int numberOfDoc = leafContext.reader().maxDoc();
                System.out.println(numberOfDoc);
                for (int docId = 0; docId < numberOfDoc; docId++) {
                    String bizid = searcher.doc(docId + startDocNo).get("business_id");
                    System.out.println(bizid);

                    fileWriter.append(bizid);
                    fileWriter.append(COMMA_DELIMITER);

                    BasicDBObject whereQuery = new BasicDBObject();
                    whereQuery.put("business_id", bizid);
                    DBCursor cursor = coll.find(whereQuery);
                    //System.out.println(cursor.next().get("categories"));

                    fileWriter.append(cursor.next().get("categories").toString().replaceAll(",", ""));
                    fileWriter.append(COMMA_DELIMITER);

                    //System.out.println(nlp.StanfordDependency(searcher.doc(docId + startDocNo).get("text")));
                    //System.out.println(searcher.doc(docId + startDocNo).get("text"));
                    List<String> top7 = s.returnTop7((nlp.StanfordDependency(searcher.doc(docId + startDocNo).get("text"))).toString());
                    System.out.println(top7);
                    for (String currbiz : top7) {
                        whereQuery.put("business_id", currbiz);
                        cursor = coll.find(whereQuery);
                        fileWriter.append(currbiz);
                        fileWriter.append(COMMA_DELIMITER);
                        fileWriter.append(cursor.next().get("categories").toString().replaceAll(",", ""));
                        fileWriter.append(COMMA_DELIMITER);
                    }
                    System.out.println();
                    fileWriter.append(NEW_LINE_SEPARATOR);
                    fileWriter.flush();
                }

            }

            fileWriter.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {


        }
    }
}
