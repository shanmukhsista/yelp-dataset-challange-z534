package main.java.indexing;

import java.io.*;
import java.util.*;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations;
import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;


public class StanfordCoreNlpDemo {
    MaxentTagger tagger;
    public StanfordCoreNlpDemo() {
        // Initialize the tagger
        tagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
    }

    public StringBuffer StanfordDependency(String query) {


        StringBuffer sb = new StringBuffer();



        // The tagged string
        String tagged = tagger.tagString(query);


        String[] words = tagged.split(" ");
        for (String s : words) {
            if (s.contains("_NN") || s.contains("_JJ") || s.contains("_RB")) {
                sb.append(s.split("_")[0]+" ");
            }

        }
        return sb;
    }

}