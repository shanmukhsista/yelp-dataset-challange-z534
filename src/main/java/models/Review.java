package models;

/**
 * Created by vivek on 11/8/15.
 */
public class Review {
    private Votes votes;
    private String user_id;
    private String review_id;
    private double stars;
    private String text_review;
    private String type;
    private String business_id;

    public Votes getVotes() {
        return votes;
    }

    public void setVotes(Votes votes) {
        this.votes = votes;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getReview_id() {
        return review_id;
    }

    public void setReview_id(String review_id) {
        this.review_id = review_id;
    }

    public double getStars() {
        return stars;
    }

    public void setStars(double stars) {
        this.stars = stars;
    }

    public String getText_review() {
        return text_review;
    }

    public void setText_review(String text_review) {
        this.text_review = text_review;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getBusiness_id() {
        return business_id;
    }

    public void setBusiness_id(String business_id) {
        this.business_id = business_id;
    }
}
