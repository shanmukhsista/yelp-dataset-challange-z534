package main.java.indexing;

import com.company.models.Tables;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by vivek on 11/21/15.
 */
public class SearchIndex {
    public List<String> GetReviews(String queryString, String table_name) {
        List<String> arr = new LinkedList<String>();
        String index = System.getProperty("user.dir") + "_" + table_name;
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths
                    .get(index)));

            IndexSearcher searcher = new IndexSearcher(reader);

            Term term = new Term("business_id", queryString);
            TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
            searcher.search(new TermQuery(term), totalHitCountCollector);


            //Print number of hits
            int count = totalHitCountCollector.getTotalHits();
            if (count > 0) {
                TopDocs topDocs = searcher.search(new TermQuery(term), count);
                //Print retrieved results
                ScoreDoc[] hits = topDocs.scoreDocs;

                for (int i = 0; i < hits.length; i++) {
                    Document doc = searcher.doc(hits[i].doc);
                    arr.add(doc.get("text"));
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arr;
    }

    public List<String> returnTop7(String queryString){
        List<String> top3=new ArrayList<String>();
        try {
            BooleanQuery.setMaxClauseCount(QueryParser.escape(queryString).length());
            String index = System.getProperty("user.dir") + "_" + Tables.REVIEW + "FINALTRAINING";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
                    .get(index)));
            IndexSearcher searcher = new IndexSearcher(reader);
            Analyzer analyzer = new StandardAnalyzer();
            searcher.setSimilarity(new BM25Similarity());

            QueryParser parser = new QueryParser("text", analyzer);
            Query query = parser.parse(QueryParser.escape(queryString));

            TopDocs results = searcher.search(query, 3);

            //Print retrieved results
            ScoreDoc[] hits = results.scoreDocs;

            for (int i = 0; i < hits.length; i++) {
                Document doc = searcher.doc(hits[i].doc);
                //System.out.println("business_id: " + doc.get("business_id"));
                top3.add(doc.get("business_id"));
            }

            reader.close();
        }catch(Exception e){
            System.out.println(e);
        }
        return top3;
    }

}
