package main.java.models.ml;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by shanmukh on 11/24/15.
 */
public class UserReviewDistance implements Comparator<UserReviewVector>, Serializable {
    public int compare(UserReviewVector o1, UserReviewVector o2) {
        if (o1.distance > o2.distance) {
            return -1;
        } else if (o1.distance < o2.distance) {
            return 1;
        } else {
            return 0;
        }
    }
}
