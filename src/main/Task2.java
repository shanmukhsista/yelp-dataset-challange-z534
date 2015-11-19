package main;

import core.ObjectSerializer;
import core.SparkManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.columnar.DOUBLE;
import readers.YelpJSONFileReader;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by shanmukh on 11/9/15.
 */
public class Task2 implements Serializable {

    public static void main(String[] main) throws Exception {

//        DataFrame usersEmptyFrame = sqlContext.createDataFrame(sc.emptyRDD(), User.class);
//        DataFrame users = getRowsForUser( "sdf",sc,yj);
        //BuildWord2VecModel(sc,yj);
        SparkManager sm = SparkManager.getInstance();
        JavaSparkContext sc = sm.getSparkContext("Task 2 Yelp");
        YelpJSONFileReader yj = new YelpJSONFileReader();
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame reviews = yj.userReviews(true, sc).limit(20);
        //For each review , search lucene index and get the tf for terms.
        final String indexDir = "bin_reviews";


        //Read lucene index and create a get tf idf for all reviews

    }

}
