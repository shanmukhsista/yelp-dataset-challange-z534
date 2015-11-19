package main.W2Vec

import java.nio.file.{Files, Paths}
import java.util

import core.PropertyManager
import gate.creole.annic.apache.lucene.index.IndexReader
import gate.creole.annic.apache.lucene.search
import main.java.models.Business
import org.apache.commons.lang.StringUtils
import org.apache.lucene.analysis.Analyzer
import org.apache.spark.mllib.linalg.{Vector}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{TextField, Field, StringField, Document}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.{FSDirectory, Directory}
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.xhtmlrenderer.layout.TextUtil
import spire.std.double

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by shanmukh on 11/16/15.
  */
@SerialVersionUID(114L)
object Task2Spark extends Serializable {
  val sconf = new SparkConf();
  sconf.setAppName("Task 2 App").set("spark.driver.allowMultipleContexts", "true");
  ;
  val sc: SparkContext = new SparkContext(sconf);
  val sqlContext: SQLContext = new SQLContext(sc)
  val pm = PropertyManager.getInstance
  val dir: Directory = FSDirectory.open(Paths.get("bin_" + "reviews"))
  val analyzer: Analyzer = new StandardAnalyzer
  val iwc: IndexWriterConfig = new IndexWriterConfig(analyzer)
  val writer: IndexWriter = new IndexWriter(dir, iwc)


  def main(args: Array[String]) {
    val userid = Array("Hh1ogFYydigptYvHeqyAog");
    var finalUserIds: Array[String] = Array.empty[String];
    //var friendsList = generateFriendsToLearn(userid,finalUserIds  , 1, 0);
    //generateDataFrameForUsers(friendsList)
    getReviews();
  }

  def getReviews() = {
    print(pm.getPropertyValue("reviewsfile"));
    var df: DataFrame = null
    System.out.println("Read json from " + pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"))
    df = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile")).limit(10)
    val reviewsSubset = df;
    val wordvec = new Word2Vec();
    val revRdd = reviewsSubset.rdd
    val reviewStringRDD = revRdd.map(r => reviewsMap(r));
    val input = reviewStringRDD.map(line => line.split(" ").toSeq);
    var model: Word2VecModel = null
    if (Files.exists(Paths.get("wordvecmodel.bin"))) {
      model = Word2VecModel.load(sc, "wordvecmodel.bin")
    }
    else {
      val word2vec = new Word2Vec()
      model = word2vec.fit(input)
      model.save(sc, "wordvecmodel.bin")
    }

    val list: scala.List[String] = scala.List("car", "hospital", "wings", "pizza", "golf", "tyre", "desert", "president");
    val srem = new StopWordsRemover();
    //tokenize before stop words removal.

    def addVector(v1: Vector, v2: Vector): Vector = {
      //v1 is the sum array
      return Vectors.dense((v1.toArray, v2.toArray).zipped.map(_ + _))
    }

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("tokenized_text")
    val broadcastVar = sc.broadcast(model);
    val finalRevviews = srem.setInputCol("tokenized_text").setOutputCol("final_col").transform(tokenizer.transform(reviewsSubset));
    finalRevviews.show(10);
    val reviewsVectorRDD = finalRevviews.map(row => {
      val localModel = broadcastVar.value;
      val tokens = row.getList[String](9);
      var vecSum = 0.0;
      var sumVector: org.apache.spark.mllib.linalg.Vector = null
      for (i <- 0 to tokens.size() - 1) {
        val token = tokens.get(i);
        try {
          if (sumVector == null) {
            sumVector = model.transform(token)
          }
          else {
            //Add it to the sum vector.
            sumVector = addVector(sumVector, model.transform(token))
          }
        } catch {
          case e: Exception => {}
        }
      }
      (row.get(2).toString, sumVector)
    })
    sqlContext.createDataFrame(reviewsVectorRDD).write.json("reviewsvector.json");
    println("Written Json!")

  }

  def reviewsMap(row: Row): String = {
    var t: String = row.get(4).toString;
    t = t.replaceAll("\\+", "")
    t = t.replaceAll("'", " ")
    t = t.replaceAll("!", "")
    t = t.replaceAll("\\?", "")
    t = t.replaceAll("\\.", " ")
    t = t.replaceAll(";", "")
    t = t.replaceAll("#", "")
    t = t.replaceAll(":", "")

    return t
  }

  /** *
    * Generates a user vector to be used by machine learning algorithm.
    * @param userid - Mine data for this given userid.
    * @param level - Level specifies the desired depth for recursion. Eg. friends only , friends of friends.
    */
  def generateFriendsToLearn(useridsToFind: Array[String], collectedUserIds: Array[String], level: Integer, currentLevel: Integer): Array[String] = {

    var newLevel: Integer = currentLevel + 1;
    val friends = getUserFriends(useridsToFind)
    //Get a users friends first.
    if (newLevel >= level) {
      return friends ++ collectedUserIds;
    }
    else {
      //Get this user's friends and go one level deep.
      return generateFriendsToLearn(friends, collectedUserIds ++ friends, level, newLevel)
    }
  }

  def generateDataFrameForUsers(users: Array[String]): DataFrame = {
    //For the given list of users, get the top n reviews for businesses rated by the user.
    //n - number of reviews to collect per user.
    val n = 4;
    var df: DataFrame = null
    val userIdString = users.mkString("','")
    val usersDataframe = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("usersfile")).filter("user_id in ('" + userIdString + "')");
    val reviewsDataFrame = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile")).filter("user_id in ('" + userIdString + "')");
    reviewsDataFrame.join(usersDataframe, usersDataframe("user_id") === reviewsDataFrame("user_id")).show(20);

    //    usersDataframe.map(row => {
    //       //business_id - 0
    //      //review_id - 2
    //      //
    //    })
    return df;


  }

  def getUserFriends(userid: Array[String]): Array[String] = {
    println("Calling function");
    //Get all the users from the json file.
    var df: DataFrame = null
    System.out.println("Read json from " + pm.getPropertyValue("jsondir") + pm.getPropertyValue("usersfile"))
    var userIdString: String = userid.mkString("','")
    df = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("usersfile")).filter("user_id in ('" + userIdString + "')");
    var friends: ListBuffer[String] = new ListBuffer[String]();
    val friendsRDD = df.flatMap(row => {
      //4th Column is the friends array column.
      var fs: util.List[String] = row.getList(4)
      fs.toArray(new Array[String](fs.size()))
    })
    //Return a list of users
    return friendsRDD.collect()
  }


  iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE)

  def ProcessReviewString(t1: String): String = {
    var t = "";
    t = t1.replaceAll("\\+", "")
    t = t1.replaceAll("'", " ")
    t = t1.replaceAll("!", "")
    t = t1.replaceAll("\\?", "")
    t = t1.replaceAll("\\.", " ")
    t = t1.replaceAll(";", "")
    t = t1.replaceAll("#", "")
    t = t1.replaceAll(":", "")
    return t;
  }

  def generateDistributedLuceneIndex() = {
    var dfBusiness: DataFrame = null
    var dfReviews: DataFrame = null
    System.out.println("Read json from " + pm.getPropertyValue("jsondir") + pm.getPropertyValue("businessesfile"))
    dfBusiness = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("businessesfile"))
    dfReviews = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"))
    dfReviews.show(10);
    val businessAgg: RDD[(String, String)] = dfReviews.map(row => (row.getString(0), row.getString(4))).reduceByKey((review1, review2) => review1 + " " + review2);
    businessAgg.foreach((e: (String, String)) => processBusinessReview(e._1, e._2));
    writer.commit();
    writer.close();
    /* val aggReviews = dfReviews.map(row => )
     //for each business ids, get the reviews and create an index.
    val businesses = df.rdd.map(row => processBusinessRow(row))
    val bkeys = businesses.map(b => (b.getId, b)).collectAsMap();*/
    //Index Reader.

  }

  def processBusinessReview(bId: String, reviewText: String) {

    print(dir.toString + "\n");
    //    print("Business ID : " + bId + " \n Agg. Review " + reviewText)
    //Generate lucene index;
    try {
      System.out.println("Table being Indexed is Business.");
      val lDoc: Document = new Document
      lDoc.add(new StringField("business_id", bId, Field.Store.YES))
      lDoc.add(new TextField("text", reviewText, Field.Store.YES))
      writer.addDocument(lDoc)
    }
    catch {
      case e: Exception => e.printStackTrace();
    }
  }
}
