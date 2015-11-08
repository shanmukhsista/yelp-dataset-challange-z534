package models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vivek on 11/8/15.
 */
public class CheckIn {
    private List<String> checkin_info;
    private String business_id;

    public String getBusiness_id() {
        return business_id;
    }

    public void setBusiness_id(String business_id) {
        this.business_id = business_id;
    }

    public List<String> getCheckin_info() {
        return checkin_info;
    }

    public void setCheckin_info(List<String> checkin_info) {
        this.checkin_info = checkin_info;
    }


}
