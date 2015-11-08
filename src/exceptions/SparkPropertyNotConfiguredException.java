package exceptions;

/**
 * Created by shanmukh on 11/1/15.
 */
public class SparkPropertyNotConfiguredException extends Exception {
    String propertyName;
    public SparkPropertyNotConfiguredException(String pName){
        this.propertyName = pName;
    }
    @Override
    public String getMessage() {
        return "Property - " + this.propertyName + " not found. \nMore Details : " ;
    }
}
