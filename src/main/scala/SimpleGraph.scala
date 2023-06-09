package de.seminar.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object SimpleGraph {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "seminar graphx")
    sc.setLogLevel("ERROR")

    val users: RDD[(VertexId, String)] = sc.parallelize(
      List(
        (1L, "Anton"),
        (2L, "Berta"),
        (3L, "Cäsar"),
        (4L, "Dora"),
        (5L, "Emil"),
        (6L, "Friedirch")))

    val relationships: RDD[Edge[String]] = sc.parallelize(
      List(
        Edge(1L, 2L, "Eltern"),
        Edge(1L, 3L, "Eltern"),
        Edge(1L, 5L, "Freund"),
        Edge(1L, 6L, "Boss"),

        Edge(2L, 3L, "Verheiratet"),
        Edge(3L, 4L, "Geschieden"),
        Edge(4L, 5L, "Verheiratet"),
        Edge(5L, 6L, "Freunde")))
    val graph = Graph(users, relationships)

    println("\nBeispiel 1")
    val marriedCount = graph.edges
      .filter { case Edge(_, _, relation) => relation == "Verheiratet" }
      .count
    println(s"Wie viele Verheiratete Paare sind im Graph? Antwort: $marriedCount")

    println("\nBeispiel 2")
    graph.triplets.map(triplet =>
      triplet.srcAttr + " ist verbunden als " + triplet.attr + " mit " + triplet.dstAttr)
      .foreach(println)

    println("\nBeispiel 3")
    val triCounts = graph.triangleCount().vertices
    val triCountByUsername = users.join(triCounts)
      .map { case (_, (username, tc)) => (username, tc)}
    println(triCountByUsername.collect().mkString("\n"))
  }
}