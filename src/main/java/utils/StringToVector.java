package main.java.utils;

import main.java.models.IndexedString;
import main.java.models.Review;
import main.java.models.StringToken;
import org.apache.commons.beanutils.converters.CharacterArrayConverter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.StopWords;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Char;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by shanmukh on 11/10/15.
 */
public class StringToVector implements Serializable {
    public DataFrame getVectorsFromString(DataFrame text, JavaSparkContext sc) throws Exception {
        //Convert this string rdd to vectors
        //DataFrame reviewsOnlyFrame =sqlContext.createDataFrame(rows, Review.class);
        StopWordsRemover rem = new StopWordsRemover();
        Tokenizer t = new Tokenizer().setInputCol("text").setOutputCol("review_text");
        rem.setInputCol("review_text").setOutputCol("text_filtered");
        JavaRDD<StringToken> rawText = rem.transform(t.transform(text)).toJavaRDD().map(new Function<Row, StringToken>() {
            public StringToken call(Row row) throws Exception {
                StringToken st = new StringToken();
                String reviewId = row.get(2).toString();
                st.setReviewId(reviewId);
                WrappedArray<String> array = (WrappedArray<String>) row.get(9);
                Iterator tokens = array.iterator();
                String r = " ";
                while (tokens.hasNext()) {
                    String t = tokens.next().toString();
                    t = t.replaceAll("\\+", "");
                    t = t.replaceAll("'", " ");
                    t = t.replaceAll("!", "");
                    t = t.replaceAll("\\?", "");
                    t = t.replaceAll("\\.", " ");
                    t = t.replaceAll(";", "");
                    t = t.replaceAll("#", "");
                    t = t.replaceAll(":", "");

                    String token = t.trim() + " ";
                    r += token;
                }
                st.setToken(r);
                return st;
            }
        });
        StringIndexer se = new StringIndexer();
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.createDataFrame(rawText, StringToken.class);
        //df.show(30);
        Tokenizer t1 = new Tokenizer().setInputCol("token").setOutputCol("tokenized");
        DataFrame df2 = t1.transform(df);
        JavaRDD<StringToken> tokens = df2.toJavaRDD().flatMap(new FlatMapFunction<Row, StringToken>() {
            public Iterable<StringToken> call(Row row) throws Exception {
                List<StringToken> words = new ArrayList<StringToken>();
                WrappedArray<String> array = (WrappedArray<String>) row.get(2);
                Iterator tokens = array.iterator();
                while (tokens.hasNext()) {
                    StringToken tk = new StringToken();

                    tk.setToken(tokens.next().toString());
                    words.add(tk);
                }
                return words;
            }
        }).filter(new Function<StringToken, Boolean>() {
            public Boolean call(StringToken stringToken) throws Exception {
                if (stringToken.getToken().isEmpty()) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        DataFrame dfTokens = sqlContext.createDataFrame(tokens, StringToken.class);
        StringIndexer si = new StringIndexer();
        DataFrame indexFrame = si.setInputCol("token").setOutputCol("text_encoded").fit(dfTokens).transform(dfTokens);
        final Map<String, Double> stringIndexes = indexFrame.toJavaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            public Tuple2<String, Double> call(Row row) throws Exception {
                double id = Double.parseDouble(row.get(2).toString());
                String token = row.get(1).toString();
                return new Tuple2<String, Double>(token, id);
            }
        }).collectAsMap();
        dfTokens.unpersist();
        final Broadcast<Map<String, Double>> bm = sc.broadcast(stringIndexes);
        JavaRDD<StringToken> mappedRDD = df2.toJavaRDD().map(new Function<Row, StringToken>() {
            Map<String, Double> index = bm.value();

            public StringToken call(Row row) throws Exception {
                WrappedArray<String> array = (WrappedArray<String>) row.get(2);
                Iterator tokens = array.iterator();
                StringToken tk = new StringToken();
                StringBuffer sb = new StringBuffer();
                while (tokens.hasNext()) {
                    String next = tokens.next().toString();
                    if (!next.isEmpty()) {
                        if (index.containsKey(next)) {
                            sb.append(index.get(next).toString());
                            sb.append(" ");
                        }
                    }
                }
                tk.setToken(sb.toString());
                tk.setReviewId(row.get(0).toString());
                sb = null;
                return tk;
            }
        });
        DataFrame finalFrame = sqlContext.createDataFrame(mappedRDD, StringToken.class);
        //Generate the original review text.
        //System.out.println("Size of dictionary " + dictionary.toString());
        return finalFrame;
    }

}
