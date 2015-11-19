package main.java;

import main.W2Vec.Task2Spark;

import java.net.UnknownHostException;

import org.apache.commons.cli.*;

/**
 * Created by shanmukh on 11/6/15.
 */

public class Main {
    public static void main(String[] args) throws UnknownHostException {
        final String RED_COLOR = "\\e[0;31m";
        final String RESET = "\\e[0m";
        CommandLineParser clip = new PosixParser();
        Options ops = constructOptions();
        CommandLine commandLine;
        try {
            //Parse command line options
            commandLine = clip.parse(ops, args);
            if (commandLine.getOptions().length == 0) {
                //WordToVec.generateDistributedLuceneIndex();
                Task2Spark.main(null);
            }
            if (commandLine.hasOption("task1")) {
                System.out.println("Executing Task 1...");
                Task1.run();
                System.out.println("Done Task 1");
            }
            if (commandLine.hasOption("task2")) {
                System.out.println("Executing task 2");
                //WordToVec.main(null);
                Task2Spark.generateDistributedLuceneIndex();
                System.out.println("Done ! ");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Invalid Option Specified . Please see the usage below. \n");
            //Print help
            PrintHelp();
            //e.printStackTrace();
        }
//

    }

    public static void PrintHelp() {
        HelpFormatter f = new HelpFormatter();
        String header = "Program Options Displayed Below \n";
        String footer = " \nEnd Help \n";
        f.setSyntaxPrefix("Usage : ");
        f.printHelp("java -jar YelpChallenge.jar", header, constructOptions(), footer, true);
    }

    public static Options constructOptions() {
        final Options op = new Options();
        op.addOption("task1", false, "Run task 1. ## Task 1 generates a lucene index and predicts the business category for a new review.");
        op.addOption("task2", false, "Run task 2. ## Task 2 recommends places to all users in the test set. ");
        return op;
    }

}
