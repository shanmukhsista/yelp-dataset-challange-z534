package core;

import exceptions.SparkPropertyNotConfiguredException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by shanmukh on 11/1/15.
 */
public class SparkManager   implements Serializable {
    public static SparkManager sm = new SparkManager();
    SparkConfigProperties appProperties ;
    private JavaSparkContext sc;
    private SparkConf conf;

    private SparkManager() {
        try{
            //Load all properties
            System.out.println("Loading properties ");
            loadPropertiesConfig();
            //Create a new spark context
            if ( conf == null){
                conf = new SparkConf(true);
                conf = this.AssignPropertiesToConfig(conf);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    public static SparkManager getInstance() throws Exception {
        return sm;
    }

    /**
     * Returns a Spark Context object by instantiating one.
     * <p>
     *     This method reads default config file for spark properties.
     *     This file should be placed in the app root directory.
     *     Name : spark-conf.properties
     * </p>
     * @return SparkContext Object
     */
    public JavaSparkContext getSparkContext(String appName){
        if ( sc == null){
            sc =  new JavaSparkContext(conf);
        }
        return sc ;
    }

    private SparkConf AssignPropertiesToConfig(SparkConf sconf) throws SparkPropertyNotConfiguredException{
        Iterator<String> properties = appProperties.getPropertyKeysIterator();
        while ( properties.hasNext()){
            String key = properties.next();
            sconf.set(key, appProperties.getPropertyValue(key));
        }
        return sconf;
    }

    private void loadPropertiesConfig() throws Exception{
        if (appProperties == null){
            appProperties = new SparkConfigProperties();
            appProperties.loadProperties();
        }
    }
}
