package nate.graphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame


abstract class Graph[Subgraph <: Graph[_]](val graph: GraphFrame, val name: String = "graph") {


  // is it possible that vertices and edges have different sqlContext?
  // It shouldn't be; and we won't allow it.
  val sqlContext = graph.edges.sqlContext

  // redundant, in a way, but for now we give the convenience accessors
  val vertices = graph.vertices
  val edges = graph.edges

  def summarize(): Unit


  def induceOnVertices(iVertices: DataFrame, inducedName : String = name + " (vertex induced)") : Subgraph
}