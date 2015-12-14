package main.W2Vec

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Collections
import java.{lang, util}

import com.google.common.collect.MinMaxPriorityQueue
import core.PropertyManager
import main.java.models.RecEvaluation
import main.java.models.ml.{UserReviewDistance, RecommendationDistance, UserReviewVector}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.JobConf
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.{Directory, FSDirectory}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by shanmukh on 11/16/15.
  */
@SerialVersionUID(114L)
object Task2Spark extends Serializable {
  val sconf = new SparkConf();
  sconf.setAppName("Task 2 App").set("spark.driver.allowMultipleContexts", "true");
  val sc: SparkContext = new SparkContext(sconf);

  val sqlContext: SQLContext = new SQLContext(sc)
  val pm = PropertyManager.getInstance
  val dir: Directory = FSDirectory.open(Paths.get("bin_" + "reviews"))
  val analyzer: Analyzer = new StandardAnalyzer
  val iwc: IndexWriterConfig = new IndexWriterConfig(analyzer)
  val writer: IndexWriter = new IndexWriter(dir, iwc)

  var reviewVectorsFrame: DataFrame = null;
  var business: DataFrame = null;
  var reviewsDataFrame: DataFrame = null;
  var usersDataframe: DataFrame = null;
  var testUsersDataFrame: DataFrame = null;
  def main(args: Array[String]) {
    val userid = Array("Hh1ogFYydigptYvHeqyAog");
    //buildFileForUser(userid)
    //getRandomRecordsFromDataset(100)
    //getReviews();
    //writeDictionary();

    reviewVectorsFrame = sqlContext.read.json("reviewsvector.json");
    business = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("businessesfile"));
    usersDataframe = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("usersfile.train"))
    testUsersDataFrame = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("usersfile.test"))
    reviewsDataFrame = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"))

    reviewVectorsFrame.cache()
    business.cache();
    usersDataframe.cache();
    testUsersDataFrame.cache();
    reviewsDataFrame.cache();


    val usersVectors = getTestUserVectors();
    val f = new File("recommendation.csv");
    FileUtils.write(f, "", false);
    //usersVectors.take(40).foreach( v => println(v.toString))
    usersVectors.take(1000).foreach(v => {
      val list = buildFileForUser(v, Array(v.getUserID))
      for (i <- 0 to list.size() - 1) {
        println("For user " + v.getUserID())
        FileUtils.write(f, v.getUserID + "," + list.get(i).toString(), true);
        println("Distance " + list.get(i).getDistance + " -> " + list.get(i).toString);
      }
    })
    //sampleUserTrainTestData();
  }

  def writeDictionary(): Unit = {
    val wordvec = new Word2Vec();
    var model: Word2VecModel = null
    if (Files.exists(Paths.get("wordvecmodel.bin"))) {
      model = Word2VecModel.load(sc, "wordvecmodel.bin")
    }
    val f = new File("dictionary.txt")
    FileUtils.write(f, "", false);
    model.getVectors.foreach(r => {
      val t = getCleanedString(r._1)
      FileUtils.write(f, t + "\t", true);
      println(t);
      val synms = model.findSynonyms(r._1, 7);
      for ((synonym, cosineSimilarity) <- synms) {
        val t = getCleanedString(synonym);
        FileUtils.write(f, (t + ","), true);
      }
      FileUtils.write(f, "\n", true);
    });
  }

  def getCleanedString(s: String): String = {
    var t = s;
    var words = Array("\\+", "'", ",", "\\", "!", "\\?", ";", "\\n", "\\r", "#", ":", "\\)", "\\(", "\\.", "\"")
    words.foreach(word => t = t.replaceAll(word, ""))
    return t;
  }

  def sampleUserTrainTestData() = {
    //create a random sample json of the user train data.
    var usersDataframe = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"));
    var splits = usersDataframe.randomSplit(Array(0.8, 0.2), seed = 1L);
    val training = splits(0);
    val testing = splits(1)
    training.repartition(1).write.json("review-train.json")
    testing.repartition(1).write.json("review-test.json")
  }

  def getReviews() = {
    var df: DataFrame = null
    df = reviewsDataFrame;
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
    val srem = new StopWordsRemover();
    //tokenize before stop words removal.

    def addVector(v1: Vector, v2: Vector): Vector = {
      //v1 is the sum array
      return Vectors.dense((v1.toArray, v2.toArray).zipped.map(_ + _))
    }

    def divideVector(v1: Vector, n: Integer): Vector = {
      //v1 is the sum array
      return Vectors.dense(v1.toArray.map(v => v / (1.0 * n)));
    }

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("tokenized_text")
    val broadcastVar = sc.broadcast(model);
    val finalRevviews = srem.setInputCol("tokenized_text").setOutputCol("final_col").transform(tokenizer.transform(reviewsSubset));
    val reviewsVectorRDD = finalRevviews.map(row => {
      val localModel = broadcastVar.value;
      val tokens = row.getList[String](9);
      var vecSum = 0.0;
      var counter: Integer = 0;
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
          counter = counter + 1;
        } catch {
          case e: Exception => {}
        }
      }
      if (sumVector != null) {
        var vectors: Array[Double] = sumVector.toArray;
        println("Counter is " + counter)
        if (counter != 0) {
          for (i <- 0 until vectors.length) {
            vectors(i) /= counter
          }
        }
        (row.get(2).toString, Vectors.dense(vectors))
      }
      else {
        (row.get(2).toString, sumVector)
      }

    })
    //print(reviewsVectorRDD.take(1));
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

  def GetUserReviewsForRecommendations() = {
    //read the recommendations.csv file and get all the unique user id's .
    val userrecs = sc.textFile("recommendation.csv");
    val distinctUserIds = userrecs.map(line => {
      val userId = line.split(',')(0);
      (userId)
    }).distinct().map(u => {
      var re = new RecEvaluation()
      re.setForUserId(u);
      (re)
    });

    val dfUsers = sqlContext.createDataFrame(distinctUserIds, classOf[RecEvaluation]);

    //For each of these users get the following
    //reviews for the user.
    val usersDataframeL = dfUsers;
    dfUsers.show(30);
    val f = new File("user_reviews_for_recommendation.csv");
    FileUtils.write(f, "", false);
    val reviewsDataFrameL = reviewsDataFrame;

    var joinedDataframe = reviewsDataFrameL.join(usersDataframeL, usersDataframeL("forUserId") === reviewsDataFrameL("user_id")).
      join(reviewVectorsFrame, reviewsDataFrameL("review_id") === reviewVectorsFrame("_1")).
      join(business, business("business_id") === reviewsDataFrameL("business_id")).foreach(row => {
      //write
      val re = new RecEvaluation();
      re.setBusinessId(row.getString(0));
      re.setForUserId(row.getString(10));
      if (row.getList[String](15) != null) {
        if (row.getList[String](15).size() != 0) {
          re.setCategories(row.getList[String](15));
        }
      }
      FileUtils.write(f, re.toString(), true);
    })


  }

  //To evaluate our algorithm, we have performed a standard cf using spark's implementation.
  //
  def performStandardCF() = {

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

  def BuildUserReviewVectorObject(row: Row, testVector: UserReviewVector): UserReviewVector = {
    var uv: UserReviewVector = new UserReviewVector()
    uv.setBusinessId(row.getString(0))
    uv.setReviewDate(row.getString(1))
    uv.setStars(row.getLong(3))
    uv.setUserID(row.getString(6))
    //Get votes vector
    uv.setFunnyVoteCount(row.getStruct(7).getLong(0))
    uv.setUseFulVoteCount((row.getStruct(7).getLong(1)))
    uv.setCoolVoteCount((row.getStruct(7).getLong(2)))
    uv.setAverageStars(row.getDouble(8))
    if (row.getList[Integer](10) != null) {
      if (row.getList[Integer](10).size() != 0) {
        uv.setElite(true)
      }
    }
    if (row.getList[String](23) != null) {
      if (row.getList[String](23).size() != 0) {
        uv.setBusinessCategories(row.getList[String](23));
      }
    }
    uv.setFans(row.getLong(11));
    uv.setFriendCount(row.getList[Integer](12).size())
    uv.setName(row.getString(13))
    uv.setReviewCount(row.getLong(14))
    uv.setUserFunnyCount(row.getStruct(17).getLong(0))
    uv.setUserUsefulCount(row.getStruct(17).getLong(1))
    uv.setUserCoolCount(row.getStruct(17).getLong(2))
    uv.setYelpingSince(row.getString(18))
    val st = row.getStruct(20)
    val dlist = st.getList[Double](1);
    uv.setTextReview(row.getString(4).replaceAll("[\\n\\r]", ""))
    var dArray = new ArrayBuffer[Double](dlist.size())
    for (i <- 0 to (dlist.size() - 1)) {
      dArray += dlist.get(i);
    }
    uv.setReviewVector(dArray.toArray);
    uv.setLatitude(row.getDouble(27))
    uv.setLongitude(row.getDouble(28))
    uv.setBusinessName(row.getString(29));

    if (testVector != null) {
      //Compute teh distance
      uv.setDistance(uv.computeDistance(uv, testVector));
    }
    return uv;
  }

  def getTestUserVectors(): Array[UserReviewVector] = {
    var joinedDataframe = reviewsDataFrame.join(testUsersDataFrame, testUsersDataFrame("user_id") === reviewsDataFrame("user_id")).
      join(reviewVectorsFrame, reviewsDataFrame("review_id") === reviewVectorsFrame("_1")).
      join(business, business("business_id") === reviewsDataFrame("business_id"));
    val finalArray = joinedDataframe.map(row => (row.getString(6), row)).reduceByKey((r1, r2) => r1).map(f => f._2).filter(r => {
      if (r.isNullAt(20)) {
        false
      } else {
        true
      }
    }).map(r => BuildUserReviewVectorObject(r, null)).collect();
    return finalArray
  }

  def buildFileForUser(uv: UserReviewVector, userId: Array[String]): util.List[UserReviewVector] = {
    val rowsToCollect: Int = 120
    //First, find the nearest vectors for the user from the entire dataset.
    val friendsLevelOneTwoWeight: Double = 0.50
    val userRatingsWeight: Double = 0.3
    val randomRows: Double = 0.2
    //Get level 1 friends
    var finalUserIds: Array[String] = Array.empty[String];
    var friendsList = generateFriendsToLearn(userId, finalUserIds, 2, 0);
    var usersToWrite: DataFrame = generateDataFrameForUsers(friendsList, (friendsLevelOneTwoWeight * rowsToCollect).toInt)
    //Append random  records from teh entire dataset.
    usersToWrite = usersToWrite.unionAll(getRandomRecordsFromDataset((randomRows * rowsToCollect).toInt))
    val rValue = usersToWrite.unionAll(getUserTopPreferences(userId, (userRatingsWeight * rowsToCollect).toInt)).rdd.filter(r => {
      if (r.isNullAt(20)) {
        false
      } else {
        true
      }
    }).map(row => BuildUserReviewVectorObject(row, uv)).toJavaRDD().takeOrdered(10, new UserReviewDistance());
    //Use this rdd ot get the max vector.
    return rValue;

    //.foreach( f => FileUtils.write(new File("recommendations_test/" +f.getUserID + ".csv"), f.toString(), true))

    //    usersToWrite.unionAll(getUserTopPreferences(userId, (userRatingsWeight * rowsToCollect).toInt)).
    //      map(row => BuildUserReviewVectorObject(row)).zipWithUniqueId().map((l: (UserReviewVector, Long)) => (l._2, l._1)).collectAsMap();
  }

  def getUserTopPreferences(users: Array[String], limit: Integer): DataFrame = {
    var df: DataFrame = null
    val userIdString = users.mkString("','")
    val usersDataframeL = usersDataframe.filter("user_id in ('" + userIdString + "')");
    val reviewsDataFrameL = reviewsDataFrame.filter("user_id in ('" + userIdString + "')");
    var joinedDataframe = reviewsDataFrameL.join(usersDataframeL, usersDataframeL("user_id") === reviewsDataFrameL("user_id")).
      join(reviewVectorsFrame, reviewsDataFrameL("review_id") === reviewVectorsFrame("_1")).
      join(business, business("business_id") === reviewsDataFrameL("business_id")).where(reviewsDataFrameL("stars") > 3 and (business("stars") > 3.5)).limit(limit);
    return joinedDataframe;
  }

  def getRandomRecordsFromDataset(count: Integer): DataFrame = {
    val reviewsDataFrameL = reviewsDataFrame.filter("stars>=4")
    var joinedDataframe = reviewsDataFrameL.join(usersDataframe, usersDataframe("user_id") === reviewsDataFrameL("user_id")).
      join(reviewVectorsFrame, reviewsDataFrameL("review_id") === reviewVectorsFrame("_1")).
      join(business, business("business_id") === reviewsDataFrameL("business_id"))
    var rCount: Long = joinedDataframe.count();
    joinedDataframe = joinedDataframe.where((business("stars") > 3.8)).sample(false, count / (1.0 * rCount)).limit(count);
    return joinedDataframe

  }

  def generateDataFrameForUsers(users: Array[String], limit: Integer): DataFrame = {
    //business_id - 0
    //date  -  1
    //stars for review 3
    // user_id 6
    //votes[3] = 7
    //average stars for user 8
    //elite 10
    //fans - 11
    //friends Array - 12
    //name - 13
    //review_count 14
    //user votes[3] =17
    //yelpiing since - 18
    //_2 review vector - 20
    //latitude - 27
    //longitude 28
    //23 - -types
    //29 name
    //For the given list of users, get the top n reviews for businesses rated by the user.
    //n - number of reviews to collect per user.
    val n = 4;
    var df: DataFrame = null
    val userIdString = users.mkString("','")
    val usersDataframeL = usersDataframe.filter("user_id in ('" + userIdString + "')");
    val reviewsDataFrameL = reviewsDataFrame.filter("user_id in ('" + userIdString + "')");
    var joinedDataframe = reviewsDataFrameL.join(usersDataframeL, usersDataframeL("user_id") === reviewsDataFrameL("user_id")).
      join(reviewVectorsFrame, reviewsDataFrameL("review_id") === reviewVectorsFrame("_1")).
      join(business, business("business_id") === reviewsDataFrameL("business_id")).where(reviewsDataFrameL("stars") > 3 and (business("stars") > 3.5))
    joinedDataframe = joinedDataframe.limit(limit);
    return joinedDataframe;
  }

  def getUserFriends(userid: Array[String]): Array[String] = {
    println("Calling function");
    //Get all the users from the json file.
    var df: DataFrame = null
    var userIdString: String = userid.mkString("','")
    df = usersDataframe.filter("user_id in ('" + userIdString + "')");
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

  def getTopWords() = {
    //Get the top 400 words from the reviews.


    val srem = new StopWordsRemover();
    //tokenize before stop words removal.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("tokenized_text")
    val reviewsTokenized = tokenizer.transform(srem.transform(reviewsDataFrame));
    reviewsTokenized.show(30);

  }
}
