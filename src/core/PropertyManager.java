package core;

import exceptions.SparkPropertyNotConfiguredException;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by shanmukh on 11/9/15.
 */
public class PropertyManager {
    static PropertyManager instance = new PropertyManager();
    String fileName = "app.properties";
    private HashMap<String, String> properties;

    private PropertyManager() {
        try {
            properties = new HashMap<String, String>();
            this.loadProperties();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static PropertyManager getInstance() {
        return instance;
    }

    public String getPropertyValue(String propertyName) throws SparkPropertyNotConfiguredException {
        if (properties.containsKey(propertyName)) {
            return properties.get(propertyName);
        } else {
            throw new SparkPropertyNotConfiguredException(propertyName);
        }
    }

    public Iterator<String> getPropertyKeysIterator() {
        return properties.keySet().iterator();
    }

    public void setProperty(String propertyName, String propertyValue) {
        properties.put(propertyName, propertyValue);
    }

    /**
     * Loads Properties from the spark-conf.properties file.
     * This method is called by the SparkManager before initiating any spark
     * context or config object.
     */
    public void loadProperties() throws Exception {
        Properties prop = new Properties();
        InputStream is = new FileInputStream(fileName);
        prop.load(is);
        Enumeration e = prop.propertyNames();
        while (e.hasMoreElements()) {
            String key = e.nextElement().toString();
            properties.put(key, prop.get(key).toString());
        }
        System.out.println("App Property Load complete...");
    }
}
