package wikipedia

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Stream.Empty

@RunWith(classOf[JUnitRunner])
class WikipediaSuite extends FunSuite with BeforeAndAfterAll {

  def initializeWikipediaRanking(): Boolean =
    try {
      WikipediaRanking
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    sc.stop()
  }

  // Conditions:
  // (1) the language stats contain the same elements
  // (2) they are ordered (and the order doesn't matter if there are several languages with the same count)
  def equivalentAndOrdered(given: List[(String, Int)], expected: List[(String, Int)]): Boolean = {
    /* (1) */ (given.toSet == expected.toSet) &&
    /* (2) */!(given zip given.tail).exists({ case ((_, occ1), (_, occ2)) => occ1 < occ2 })
  }

//  test("'occurrencesOfLang' should work for Empty RDD") {
//    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
//    import WikipediaRanking._
//    val rdd: RDD[WikipediaArticle] = sc.emptyRDD
//    val res = (occurrencesOfLang("Java", rdd) == 0)
//    assert(res, "occurrencesOfLang given (specific) empty RDD should equal to 0")
//  }
//
//  test("'occurrencesOfLang' should work for (specific) RDD with one element") {
//    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
//    import WikipediaRanking._
//    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
//    val res = (occurrencesOfLang("Java", rdd) == 1)
//    assert(res, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
//  }
//
//  test("'occurrencesOfLang' shouldn't count languages that don't occur") {
//    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
//    import WikipediaRanking._
//    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta"),WikipediaArticle("title", "Java Jakarta")))
//    val res = (occurrencesOfLang("PHP", rdd) == 0)
//    assert(res, "occurrencesOfLang, if language isn't found, should equal to 0")
//  }
//
//  test("'rankLangs' run on the dataset provided in object `WikipediaData` returns the correct ranking (in descending order)") {
//      assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
//      import WikipediaRanking._
//      val langs = List("Scala", "Java")
//
//      val articles = List(
//            WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
//            WikipediaArticle("2","Scala and Java run on the JVM"),
//            WikipediaArticle("3","Scala is not purely functional")
//          )
//          val rdd = sc.parallelize(articles)
//          val ranked = rankLangs(langs, rdd)
//          val res = ranked.head._1 == "Scala"
//          assert(res)
//  }
//
//  test("'makeIndex' creates a simple index with two entries") {
//      assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
//      import WikipediaRanking._
//      val langs = List("Scala", "Java")
//      val articles = List(
//          WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
//          WikipediaArticle("2","Scala and Java run on the JVM"),
//          WikipediaArticle("3","Scala is not purely functional")
//        )
//      val rdd = sc.parallelize(articles)
//      val index = makeIndex(langs, rdd)
//      val res = index.count() == 2
//      assert(res)
//  }

  test("'rankLangsUsingIndex' run on a subset of the dataset provided in object `WikipediaData` returns the correct " +
      "ranking (in descending order)") {
      assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
      import WikipediaRanking._
      val langs = List("Java", "Scala","JavaScript")
      val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","JavaScript is for front"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional")
      )
      val rdd = sc.parallelize(articles)
      val index = makeIndex(langs, rdd)
      val ranked = rankLangsUsingIndex(index)
      val res = (ranked.head._1 == "Scala")
      assert(res)
  }

//  test("'rankLangsUsingIndex' should work for a simple RDD with three elements") {
//      assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
//      import WikipediaRanking._
//      val langs = List("Scala", "Java")
//      val articles = List(
//          WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
//          WikipediaArticle("2","Scala and Java run on the JVM"),
//          WikipediaArticle("3","Scala is not purely functional")
//        )
//      val rdd = sc.parallelize(articles)
//      val index = makeIndex(langs, rdd)
//      val ranked = rankLangsUsingIndex(index)
//      val res = (ranked.head._1 == "Scala")
//      assert(res)
//  }
//
//  test("'rankLangsReduceByKey' should work for a simple RDD with four elements") {
//      assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
//      import WikipediaRanking._
//      val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
//      val articles = List(
//          WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
//          WikipediaArticle("2","Scala and Java run on the JVM"),
//          WikipediaArticle("3","Scala is not purely functional"),
//          WikipediaArticle("4","The cool kids like Haskell more than Java"),
//          WikipediaArticle("5","Java is for enterprise developers")
//        )
//      val rdd = sc.parallelize(articles)
//      val ranked = rankLangsReduceByKey(langs, rdd)
//      val res = (ranked.head._1 == "Java")
//      assert(res)
//  }
}
