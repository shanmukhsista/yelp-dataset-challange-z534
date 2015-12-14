package main.java.models;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shanmukh on 12/4/15.
 */
public class RecEvaluation {
    String forUserId;
    String businessId;
    List<String> categories;

    public String getForUserId() {
        return forUserId;
    }

    public void setForUserId(String forUserId) {
        this.forUserId = forUserId;
    }

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        //Name
        List<String> row = new ArrayList<String>();
        row.add(StringEscapeUtils.escapeCsv(this.forUserId));
        row.add(StringEscapeUtils.escapeCsv(this.businessId));
        row.add(StringEscapeUtils.escapeCsv(StringUtils.join(this.getCategories(), "#")));
        return StringUtils.join(row, ",") + "\n";
    }
}
