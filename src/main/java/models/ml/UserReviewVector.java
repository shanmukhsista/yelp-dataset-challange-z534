package main.java.models.ml;

import com.google.common.primitives.Doubles;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by shanmukh on 11/12/15.
 */
public class UserReviewVector implements Serializable {
    public boolean isElite;
    long yelpingSince;
    long funnyVoteCount;
    long useFulVoteCount;
    long coolVoteCount;
    long reviewCount;
    int friendCount;
    long fans;
    double distance = Double.POSITIVE_INFINITY;
    double averageStars;
    String topKeywords;
    long stars;
    double longitude;
    double latitude;
    //Rading
    double textVector;
    String textReview;
    String businessId;
    long reviewDate;
    String userID;
    String name;
    long userFunnyCount;
    long userUsefulCount;
    long userCoolCount;
    String businessName;
    List<String> businessCategories;
    String address;
    double[] reviewVector;

    public UserReviewVector() {
        this.isElite = false;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public long getFunnyVoteCount() {
        return funnyVoteCount;
    }

    public void setFunnyVoteCount(long funnyVoteCount) {
        this.funnyVoteCount = funnyVoteCount;
    }

    public long getYelpingSince() {
        return yelpingSince;
    }

    public void setYelpingSince(String yelpingSince) {
        //parse date string and convert to datetimg
        try {

            DateTimeFormatter f = DateTimeFormat.forPattern("Y-MM");
            this.yelpingSince = f.parseDateTime(yelpingSince).getMillis();
        } catch (Exception e) {
            this.yelpingSince = 0;
        }

    }

    public long getUseFulVoteCount() {
        return useFulVoteCount;
    }

    public void setUseFulVoteCount(long useFulVoteCount) {
        this.useFulVoteCount = useFulVoteCount;
    }

    public long getCoolVoteCount() {
        return coolVoteCount;
    }

    public void setCoolVoteCount(long coolVoteCount) {
        this.coolVoteCount = coolVoteCount;
    }

    public long getReviewCount() {
        return reviewCount;
    }

    public void setReviewCount(long reviewCount) {
        this.reviewCount = reviewCount;
    }

    public int getFriendCount() {
        return friendCount;
    }

    public void setFriendCount(int friendCount) {
        this.friendCount = friendCount;
    }

    public long getFans() {
        return fans;
    }

    public void setFans(long fans) {
        this.fans = fans;
    }

    public double getAverageStars() {
        return averageStars;
    }

    public void setAverageStars(double averageStars) {
        this.averageStars = averageStars;
    }

    public String getTopKeywords() {
        return topKeywords;
    }

    public void setTopKeywords(String topKeywords) {
        this.topKeywords = topKeywords;
    }

    public long getStars() {
        return stars;
    }

    public void setStars(long stars) {
        this.stars = stars;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getTextVector() {
        return textVector;
    }

    public void setTextVector(double textVector) {
        this.textVector = textVector;
    }

    public String getTextReview() {
        return textReview;
    }

    public void setTextReview(String textReview) {
        this.textReview = textReview;
    }

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public long getReviewDate() {
        return reviewDate;
    }

    public void setReviewDate(String reviewDate) {
        //parse date string and convert to datetimg
        try {

            DateTimeFormatter f = DateTimeFormat.forPattern("Y-MM-dd");
            this.reviewDate = f.parseDateTime(reviewDate).getMillis();
        } catch (Exception e) {
            this.reviewDate = 0;
        }
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public boolean isElite() {
        return isElite;
    }

    public void setElite(boolean elite) {
        isElite = elite;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBusinessName() {
        return businessName;
    }

    public void setBusinessName(String businessName) {
        this.businessName = businessName;
    }

    public List<String> getBusinessCategories() {
        return businessCategories;
    }

    public void setBusinessCategories(List<String> businessCategories) {
        this.businessCategories = businessCategories;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public long getUserFunnyCount() {
        return userFunnyCount;
    }

    public void setUserFunnyCount(long userFunnyCount) {
        this.userFunnyCount = userFunnyCount;
    }

    public long getUserUsefulCount() {
        return userUsefulCount;
    }

    public void setUserUsefulCount(long userUsefulCount) {
        this.userUsefulCount = userUsefulCount;
    }

    public long getUserCoolCount() {
        return userCoolCount;
    }

    public void setUserCoolCount(long userCoolCount) {
        this.userCoolCount = userCoolCount;
    }

    public double[] getReviewVector() {
        return reviewVector;
    }

    public void setReviewVector(double[] reviewVector) {
        this.reviewVector = reviewVector;
    }

    public double computeDistance(UserReviewVector o1, UserReviewVector o2) {
        double distance = 0.0;
        distance += getAbsoluteSquare(o1.yelpingSince, o2.yelpingSince) +
                getAbsoluteSquare(o1.averageStars, o2.averageStars) +
                getAbsoluteSquare(o1.funnyVoteCount, o2.funnyVoteCount) +
                getAbsoluteSquare(o1.coolVoteCount, o2.coolVoteCount) +
                getAbsoluteSquare(o1.useFulVoteCount, o2.useFulVoteCount) +
                getAbsoluteSquare(o1.fans, o2.fans) +
                getAbsoluteSquare(o1.getLatitude(), o2.getLatitude()) +
                getAbsoluteSquare(o1.getLongitude(), o2.getLongitude()) +
                getAbsoluteSquare(o1.getReviewCount(), o2.getReviewCount()) +
                getAbsoluteSquare(o1.getReviewDate(), o2.getReviewDate()) +
                getAbsoluteSquare(o1.getFriendCount(), o2.getFriendCount()) +
                getAbsoluteSquare(o1.isElite ? 1 : 0, o2.isElite ? 1 : 0);
        return (Math.sqrt(distance) * computeCosineSimilarity(o1.getReviewVector(), o2.getReviewVector()));
        //Get cosine similarity for vectors
    }

    public double getAbsoluteSquare(double v1, double v2) {
        return Math.pow((v1 - v2), 2);
    }

    public double computeCosineSimilarity(double[] v1, double[] v2) {
        double ans = 0.0;
        for (int i = 0; i < v1.length; i++) {
            ans = ans + (v1[i] * v2[i]);
        }
        double moda = 0.0;
        for (int i = 0; i < v1.length; i++) {
            moda = moda + (v1[i] * v1[i]);
        }
        double modb = 0.0;
        for (int i = 0; i < v1.length; i++) {
            modb = modb + (v2[i] * v2[i]);
        }
        //
        return (ans / Math.sqrt(moda * modb));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        //Name
        List<String> row = new ArrayList<String>();
        row.add(StringEscapeUtils.escapeCsv(this.businessName));
        row.add(StringEscapeUtils.escapeCsv(StringUtils.join(this.getBusinessCategories(), "#")));
        row.add(StringEscapeUtils.escapeCsv(this.businessId));
        row.add(StringEscapeUtils.escapeCsv(this.name));
        //yelping since
        row.add("" + this.yelpingSince);
        //votes funnly
        row.add(this.funnyVoteCount + "");
        //votesuseful
        row.add(this.useFulVoteCount + "");
        //votescool
        row.add(this.coolVoteCount + "");
        //reviewcount
        row.add(this.reviewCount + "");
        //friendcount
        row.add(this.friendCount + "");
        //
        row.add(this.stars + "");
        //average stars
        row.add(this.averageStars + "");
        row.add(this.fans + "");
        row.add(this.latitude + "");
        row.add(this.longitude + "");
        //vector for review text
        List<Double> vectors = Doubles.asList(this.reviewVector);
        row.add(StringUtils.join(vectors, ","));
        row.add(StringEscapeUtils.escapeCsv(this.textReview));
        return StringUtils.join(row, ",") + "\n";
    }
}
