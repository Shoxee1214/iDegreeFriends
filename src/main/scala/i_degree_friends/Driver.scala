package i_degree_friends

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * Hello world!
 *
 */
object Driver {

  def main(args: Array[String]): Unit = {

    // 2 input arguments are compulsory: inputpath of file & N. 3rd argument is optional which can be hdfs path where output can be stored as a file
    // if N <= 0 then program will not result in anything
    if (args.size < 2 | args.size > 3 | args(1).toInt <= 0) {
      println("Bad Arguments")
      return

    }

    // Initializing Spark Context
    val sc = new SparkContext("local[*]", "i-degree-friends")

    // Ingest file as RDD of Strings
    val rdd = sc.textFile(args(0))

    // transformed data of (a  b) tuples to also contain (b a) tuple so that we can calculate friends for every person later
    val pairRDD = rdd.flatMap { x =>
      val map = x.split(Constants.SEPARATOR)
      Seq(
        (map(0), map(1)),
        (map(1), map(0)))
    }

    // Grouped data according to key so that now we'll have a person and corresponding list of friends in every row of data
    var friendsRDD = pairRDD.groupByKey
    friendsRDD.foreach { println }

    // Map of k,v where k is a person and v is a list of k's friends. also broadcasting this on every executor of spark cluster so that we can
    // use this for finding friends
    val friendsDictionary = sc.broadcast(friendsRDD.collectAsMap().toMap)

    // broadcasting the N across all executors
    val N = sc.broadcast(args(1).toInt)

    // if N>1 then following will run otherwise we will output those 1 degree friends in friendsRDD
    if (N.value > 1) {

      val iDegreeFriends = friendsRDD.map { case (person, friends) =>
        person + Constants.SEPARATOR + Utils.findiDegreeFriends(person, friends.toSeq, friendsDictionary.value, N.value)
      }.sortBy(_.split(Constants.SEPARATOR)(0), true)

      // to print on console in order, we'll have to collect RDD otherwise each partition can be printed out of order
      iDegreeFriends.collect.foreach(println)

      // if outpath path is specified in input arguments then the resulting output will be saved in a file
      if (args.size == 3) {
        iDegreeFriends.coalesce(1).saveAsTextFile(args(2))
      }
    } else {

      val iDegreeFriends = friendsRDD.map { case (person, friends) =>
        person + Constants.SEPARATOR + friends.toSeq.sorted.mkString(Constants.SEPARATOR)
      }.sortBy(_.split(Constants.SEPARATOR)(0), true)

      iDegreeFriends.collect.foreach(println)

      if (args.size == 3) {
        iDegreeFriends.coalesce(1).saveAsTextFile(args(2))
      }

    }

  }

}
