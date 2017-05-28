package i_degree_friends




/**
 * @author shahroz
 */
object Utils extends Serializable {

  def findiDegreeFriends(key: String, initialFriends: Seq[String], friendsDictionary: Map[String, Iterable[String]], N: Int): String = {

    // key: the person whose friends we are going to find
    // initial friends: 1 degree friends of a person
    // friendsDictionary: the Map which have each person as key and their corresponding friends as list
    // N: degree upto which we have to find friends
    // output of this function: function will return the i-degree-friends of a specific person in sorted order as tab separated string

    // Program will come to this function when N will be greater than 1
    // we are doing following here
    // in each iteration we'll have three collections
    // 1) total ith-degree-friends of a person
    // 2) friends at i-plus-level
    // 3) friends at i-minus-level. I'm saving this because whenever we'll do breadth first search then we'll remove that element from consideration which had already occurred to save time

    var i = 1
    var iLevelFriends = initialFriends
    var iMinusLevelFriends = Seq(key)
    var iDegreeFriends = iLevelFriends

    do {
      i += 1

      var iplusLevelFriends = iLevelFriends.flatMap { name =>
        friendsDictionary(name)
      }.toSeq

      val temp = iplusLevelFriends.distinct.diff(iMinusLevelFriends)
      iDegreeFriends = iDegreeFriends ++ temp
      iMinusLevelFriends = iLevelFriends.toSeq
      iLevelFriends = temp
    } while (i < N)

    iDegreeFriends.sorted.mkString(Constants.SEPARATOR)
  }
}