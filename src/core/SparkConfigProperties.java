package core;
import exceptions.SparkPropertyNotConfiguredException;

import javax.print.DocFlavor;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by shanmukh on 11/1/15.
 */
public class SparkConfigProperties implements Serializable {
    private HashMap<String, String> properties;
    public SparkConfigProperties(){
        properties = new HashMap<String, String>();
    }
    public String getPropertyValue(String propertyName) throws SparkPropertyNotConfiguredException{
            if ( properties.containsKey(propertyName)){
                return properties.get(propertyName);
            }
            else{
                throw new SparkPropertyNotConfiguredException(propertyName);
            }
    }
    public Iterator<String> getPropertyKeysIterator(){
        return properties.keySet().iterator();
    }
    public void setProperty(String propertyName, String propertyValue){
        properties.put(propertyName , propertyValue);
    }
    /**
     * Loads Properties from the spark-conf.properties file.
     * This method is called by the SparkManager before initiating any spark
     * context or config object.
     */
    public void loadProperties() throws Exception{
        Properties prop = new Properties();
        InputStream is = new FileInputStream("spark-conf.properties");
        prop.load(is);
        Enumeration e = prop.propertyNames();
        while( e.hasMoreElements()){
            String key = e.nextElement().toString();
            properties.put(key, prop.get(key).toString());
        }
        System.out.println("Property Load complete...");
    }
}
