package main.java;

import main.java.indexing.GenerateLuceneIndex;
import main.java.models.Tables;

/**
 * Created by shanmukh on 11/16/15.
 */
public class Task1 {
    public Task1() {

    }

    public static void run() {
        System.out.println("===Indexing==");
        GenerateLuceneIndex gen = new GenerateLuceneIndex(Tables.REVIEW);
        gen = new GenerateLuceneIndex(Tables.TIP);
    }
}
