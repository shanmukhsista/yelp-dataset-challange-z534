package main.java.indexing;

import com.company.models.Tables;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by vivek on 11/21/15.
 */
public class ParseIndex {
    public void stats(String table_name) {
        String index = System.getProperty("user.dir") + "_" + table_name;
        System.out.println(index);
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));


            System.out.println("Total number of documents in the corpus: "
                    + reader.maxDoc());

            Terms vocabulary = MultiFields.getTerms(reader, "text");
            System.out.println("Size of the vocabulary for this field: "
                    + vocabulary.size());

            System.out
                    .println("Number of documents that have at least one term for this field: "
                            + vocabulary.getDocCount());

            System.out.println("Number of tokens for this field: "
                    + vocabulary.getSumTotalTermFreq());

            System.out.println("Number of postings for this field: "
                    + vocabulary.getSumDocFreq());

            TermsEnum iterator = vocabulary.iterator();
            BytesRef byteRef = null;
            System.out.println("\n*******Vocabulary-Start**********");
            while ((byteRef = iterator.next()) != null) {
                String term = byteRef.utf8ToString();
                //System.out.println(term);
            }
            System.out.println("\n*******Vocabulary-End**********");

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
