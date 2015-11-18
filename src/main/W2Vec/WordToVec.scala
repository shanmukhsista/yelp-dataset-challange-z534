package main.W2Vec

import java.nio.file.{Files, Paths}
import java.util

import core.PropertyManager
import gate.creole.annic.apache.lucene.index.IndexReader
import gate.creole.annic.apache.lucene.search
import main.java.models.Business
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{TextField, Field, StringField, Document}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.{FSDirectory, Directory}
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, Word2VecModel}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by shanmukh on 11/16/15.
  */
@SerialVersionUID(114L)
object WordToVec extends Serializable {
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
    getReviews();
  }

  def getReviews() = {
    print(pm.getPropertyValue("reviewsfile"));
    var df: DataFrame = null
    System.out.println("Read json from " + pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"))
    df = sqlContext.read.json(pm.getPropertyValue("jsondir") + pm.getPropertyValue("reviewsfile"))
    val reviewsSubset = df;
    val wordvec = new Word2Vec();
    val revRdd = reviewsSubset.rdd
    val reviewStringRDD = revRdd.map(r => reviewsMap(r));
    val input = reviewStringRDD.map(line => line.split(" ").toSeq);
    var model: feature.Word2VecModel = null
    if (Files.exists(Paths.get("wordvecmodel.bin"))) {
      model = Word2VecModel.load(sc, "wordvecmodel.bin")
    }
    else {
      val word2vec = new Word2Vec()
      model = word2vec.fit(input)
      model.save(sc, "wordvecmodel.bin")
    }

    val list: scala.List[String] = scala.List("car", "hospital", "wings", "pizza", "golf", "tyre", "desert", "president");
    //var syn:Array[(String, Double)] = null;
    for (word <- list; synonyms = model.findSynonyms(word, 5)) {
      print("***************\n Similar words to " + word + "  are : \n");
      for ((synonym, cosineSimilarity) <- synonyms) {
        println(synonym + " " + cosineSimilarity);
      }
      print("***************\n")
    }
    //
    //    for((synonym, cosineSimilarity) <- synonyms) {
    //      println("$synonym $cosineSimilarity");
    //    }
    //  }
  }

  iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE)

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
