package i_degree_friends

import org.junit._
import org.junit.Assert._
import scala.Iterable

@Test
class DriverTest {

  @Test
  def testFindDegreeFriends() {

    val rawData = Seq("davidbowie omid",
      "davidbowie kim",
      "kim  torsten",
      "torsten  omid",
      "brendan  torsten",
      "ziggy  davidbowie",
      "mick ziggy")

    val key = "kim"
    val initialFriends = Seq("davidbowie", "torsten")
    val N = 2
    val friendsDictionary = Map("brendan" -> Iterable("torsten"),
      "mick" -> Iterable("ziggy"),
      "torsten" -> Iterable("kim", "omid", "brendan"),
      "omid" -> Iterable("davidbowie", "torsten"),
      "ziggy" -> Iterable("davidbowie", "mick"),
      "kim" -> Iterable("davidbowie", "torsten"),
      "davidbowie" -> Iterable("omid", "kim", "ziggy"))

    val output = Seq("brendan", "davidbowie", "omid", "torsten", "ziggy").mkString(Constants.SEPARATOR)

    assertEquals(output, Utils.findiDegreeFriends(key, initialFriends, friendsDictionary, N))

  }

}


