package main.java;

import core.ObjectSerializer;
import main.java.utils.StringToVector;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.ObjectInputStreamWithLoader;
import readers.YelpJSONFileReader;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by shanmukh on 11/16/15.
 */
public class Test {
    static ObjectSerializer os = new ObjectSerializer();

    public static void buildLDAModel(JavaSparkContext sc, YelpJSONFileReader yj) throws Exception {
        DataFrame f = yj.userReviews(true, sc);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        StringToVector vec = new StringToVector();
        DataFrame reviewFrame = vec.getVectorsFromString(f, sc);
        JavaRDD<Row> returnRDD = reviewFrame.toJavaRDD();
        Map<String, Long> idMap = returnRDD.map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                String id = row.get(0).toString();
                return id;
            }
        }).zipWithUniqueId().collectAsMap();
        final Broadcast<Map<String, Long>> bm = sc.broadcast(idMap);
        JavaPairRDD<Long, Vector> ldaData = returnRDD.mapToPair(new PairFunction<Row, Long, Vector>() {
            Map<String, Long> ids = bm.getValue();

            public Tuple2<Long, Vector> call(Row row) throws Exception {
                String id = row.get(0).toString();
                String tokens = row.get(1).toString();
                String[] numTokens = tokens.split(" ");
                double[] numbers = new double[numTokens.length];
                for (int i = 0; i < numTokens.length; i++) {
                    numbers[i] = Double.parseDouble(numTokens[i]);
                }
                return new Tuple2<Long, Vector>(ids.get(id), Vectors.dense(numbers));

            }
        });
        LDAModel ldaModel = new LDA().setK(10).run(ldaData);
        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                + " words):");
        Matrix topics = ldaModel.topicsMatrix();
        for (int topic = 0; topic < 3; topic++) {
            System.out.print("Topic " + topic + ":");
            for (int word = 0; word < ldaModel.vocabSize(); word++) {
                System.out.print(" " + topics.apply(word, topic));
            }
            System.out.println();
        }

        System.exit(2);
        f = f.select("text");
        f.show(10);
    }

    public static DataFrame getRowsForUser(String userId, JavaSparkContext sc, YelpJSONFileReader yj) {
        //Get all the rows for a given user.
        userId = "Xqd0DzHaiyRqVH3WRG7hzg";
        int rowsToCollect = 300;
        //First, find the nearest vectors for the user from the entire dataset.
        double friendsLevelOneWeight = 0.50;
        double friendsLevelTwoWeight = 0.25;
        double userRatingsWeight = 0.17;
        double randomRows = 0.15;
        //
        DataFrame usersFrame = yj.getUsers(sc);
        DataFrame filtered = usersFrame.filter(usersFrame.col("user_id").equalTo(userId));
        //find friends of this
        WrappedArray<String> array = (WrappedArray<String>) filtered.collect()[0].get(4);
        List<String> friends = new ArrayList<String>();
        Iterator tokens = array.iterator();
        while (tokens.hasNext()) {
            friends.add(tokens.next().toString());
        }
        System.out.println(friends);
        getTopFriendsForUsers(friends, sc, yj);
        //Now get the most visited places for t
        return filtered;
    }

    public static DataFrame getTopFriendsForUsers(List<String> userIds, JavaSparkContext sc, YelpJSONFileReader yj) {
        //Select rows from the database which have rating more than 3.5.
        double ratingThreshhold = 3.5;
        DataFrame usersFrame = yj.getBusinessesForUsers(userIds, sc, 500);
        usersFrame.show();
        System.out.println("Row coutn " + usersFrame.count());
        usersFrame.toJavaRDD().foreach(new VoidFunction<Row>() {
            public void call(Row row) throws Exception {
                System.out.println(row.get(4).toString());
            }
        });
        return usersFrame;
    }

    public static void BuildWord2VecModel(JavaSparkContext sc, YelpJSONFileReader yj) throws Exception {
        Word2VecModel m = null;
        if (os.canDeserialize("word2vecmodel.bin")) {
            System.out.println("Deserializing model");

        } else {
            DataFrame userReviews = yj.userReviews(true, sc);
            userReviews.select("text").show(23);
            //Use this dataframe to build a word 2 vec model.
            Word2Vec w = new Word2Vec();
            Tokenizer t = new Tokenizer();
            DataFrame tokenReviews = t.setInputCol("text").setOutputCol("text_tokenized").transform(userReviews);
            tokenReviews.cache();
            os.serializeObject(m, "word2vecmodel.bin");
        }
        System.out.println("Finding synonyms of garlic ");
        //System.out.println("input " + m.findSynonyms("food", 10));
    }

    public static List<String> getBusinessTypes(JavaSparkContext sc, YelpJSONFileReader yj) {
        List<String> businesses = null;
        DataFrame bSet = yj.getBusinessDataSet(sc);
        DataFrame categoriesFrame = bSet.select("categories");
        businesses = categoriesFrame.toJavaRDD().flatMap(new FlatMapFunction<Row, String>() {
            public Iterable<String> call(Row row) throws Exception {
                List<String> cats = new ArrayList<String>();
                WrappedArray<String> array = (WrappedArray<String>) row.get(0);
                Iterator tokens = array.iterator();
                while (tokens.hasNext()) {
                    cats.add(tokens.next().toString());
                }
                return cats;
            }
        }).distinct().collect();
        return businesses;
    }
}
