package readers;

import core.PropertyManager;
import main.java.models.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.dmg.pmml.Value;

import java.io.Serializable;
import java.util.List;

/**
 * Created by shanmukh on 11/8/15.
 */
public class YelpJSONFileReader implements Serializable {
    /**
     * Reads the raw json files for Apache Spark.
     */
    PropertyManager pm;

    public YelpJSONFileReader() {
        pm = PropertyManager.getInstance();
    }

    public DataFrame userReviews(boolean removeStopWords, JavaSparkContext sc) {
        DataFrame df = null;
        try {
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
            System.out.println("Read json from " + pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"));
            df = sqlContext.read().json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return df;
    }

    public DataFrame getBusinessDataSet(JavaSparkContext sc) {
        DataFrame df = null;
        try {
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
            System.out.println("Read json from " + pm.getPropertyValue("jsondir") + pm.getPropertyValue("businessesfile"));
            df = sqlContext.read().json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("businessesfile"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return df;
    }

    public DataFrame getUsers(JavaSparkContext sc) {
        DataFrame df = null;
        try {
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
            System.out.println("Read json from " + pm.getPropertyValue("jsondir") + pm.getPropertyValue("usersfile"));
            df = sqlContext.read().json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("usersfile"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return df;
    }

    public DataFrame getBusinessesForUsers(List<String> friends, JavaSparkContext sc, int limit) {
        DataFrame df = null;
        try {
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
            System.out.println("Read json from " + pm.getPropertyValue("jsondir") + pm.getPropertyValue("usersfile"));
            df = sqlContext.read().json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"));
            df = df.where("user_id in ('" + StringUtils.join(friends, "','") + "') and stars > 3").sort(df.col("stars").desc()).limit(limit);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return df;
    }

}
