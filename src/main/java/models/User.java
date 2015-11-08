package models;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by vivek on 11/8/15.
 */
public class User {
    private Date yelping_since;
    private Votes votes;
    private double review_count;
    private String name;
    private String user_id;
    private List<String> friends;
    private double fans;
    private double average_stars;
    private String type;
    private List<Map<String, Double>> compliments;
    private List<Map<Double, Double>> elite;

    public Date getYelping_since() {
        return yelping_since;
    }

    public void setYelping_since(Date yelping_since) {
        this.yelping_since = yelping_since;
    }

    public Votes getVotes() {
        return votes;
    }

    public void setVotes(Votes votes) {
        this.votes = votes;
    }

    public double getReview_count() {
        return review_count;
    }

    public void setReview_count(double review_count) {
        this.review_count = review_count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public List<String> getFriends() {
        return friends;
    }

    public void setFriends(List<String> friends) {
        this.friends = friends;
    }

    public double getFans() {
        return fans;
    }

    public void setFans(double fans) {
        this.fans = fans;
    }

    public double getAverage_stars() {
        return average_stars;
    }

    public void setAverage_stars(double average_stars) {
        this.average_stars = average_stars;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Map<String, Double>> getCompliments() {
        return compliments;
    }

    public void setCompliments(List<Map<String, Double>> compliments) {
        this.compliments = compliments;
    }

    public List<Map<Double, Double>> getElite() {
        return elite;
    }

    public void setElite(List<Map<Double, Double>> elite) {
        this.elite = elite;
    }
}
