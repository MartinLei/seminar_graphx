package de.seminar.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

object TriangleCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "seminar graphx")

    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/exampleGraph/followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    val triCounts = graph.triangleCount().vertices

    val users = sc.textFile("src/main/resources/exampleGraph/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    val triCountByUsername = users.join(triCounts)
      .map { case (id, (username, tc)) => (username, tc) }

    println(triCountByUsername.collect().mkString("\n"))
  }
}
