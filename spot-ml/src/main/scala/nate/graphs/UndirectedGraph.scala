package nate.graphs


import nate.GraphFrameUtilities
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import nate.Schema._

import scala.math.pow



/**
  * Represenation of undirected graph, posisbly with loops, but no edge weights or multiplicities.
  *
  * It just wraps a graph frame and provides some methods for interpreting it as an undirected graph.
  *
  * Undirected graph has edges represented with SRC <= DST (w.r.t. string comparison)
  *
  * @param graph graph frame
  * @param name
  */
class UndirectedGraph(graph: GraphFrame, name: String = "undirected graph") extends Graph[UndirectedGraph](graph, name) {



  def summarize() : Unit = {


    val vCount = graph.vertices.count()
    val eCount = graph.edges.count()
    val maxEdges = vCount * vCount.toDouble / 2d


    println("Undirected Graph: " + name)
    println("vertex count:  " + vCount)

    println("edge count:  " + eCount + "  out of a possible " + maxEdges)

    println("edge density:   " +  eCount.toDouble / maxEdges)


    // here we use the construction that edges are present in a bidirectional form
    val degrees : DataFrame = graph.degrees.cache()

    degrees.registerTempTable("degrees")



    val maxDegreeDF = sqlContext.sql("SELECT MAX(degree) as maxdegree FROM degrees")
    val maxDegree = maxDegreeDF.take(1)(0).getInt(0)
    println("MAXIMUM DEGREE: " + maxDegree)

    val ceilLog2MaxDegree = Math.ceil(scala.math.log(maxDegree) / scala.math.log(2)).toInt

    def expDegreeBucket(df: DataFrame, i: Int) = df.filter("degree >= " + pow(2, i)).filter("degree < " + pow(2, i + 1))
    Range(0, ceilLog2MaxDegree).toList.foreach(i => {
      val c = expDegreeBucket(degrees, i).count()
      println("vertex count with degree in range  " + pow(2, i) + " <= degree < " + pow(2, i + 1) + ": " + c)
    })

  }


  def induceOnVertices(iVertices: DataFrame, inducedName : String = name + " (vertex induced)") : UndirectedGraph = {
    new UndirectedGraph(GraphFrameUtilities.vertexInducedSubgraph(graph, iVertices).cache(), inducedName)
  }


  def clusteringCoefficient() : DataFrame = {
    val triangleCount = graph.triangleCount.run()
    val TRIANGLECOUNT = "count"

    val degrees = graph.degrees
    val DEGREE= "degree"

    val joinedFrame = degrees
      .join(triangleCount, degrees(ID) === triangleCount(ID))
      .drop(degrees(ID))

    val msg = udf { (triangles: Long, degree: Int) => if (degree <= 1) 1.0d else (triangles * 2).toDouble / (degree.toLong * (degree.toLong - 1)).toDouble }

    joinedFrame.withColumn(CLUSTERING_COEFFICIENT, msg(col(TRIANGLECOUNT), col(DEGREE))).select(ID, CLUSTERING_COEFFICIENT).cache()
  }

  def dropHighDegreeNodes(maxDegree: Long, outName: String = name + "_degreefiltered"): UndirectedGraph = {
    val oldEdges = graph.edges
    val oldVertices = graph.vertices


    // Graph Frames has an annoying feature that it drops isolated vertices from the degree frames
    // only thing to do is take that left outer join to get them back in then imput the nulls to 0

    val vertexDegrees = oldVertices.join(graph.degrees, oldVertices(ID) === graph.degrees(ID), "left_outer")
        .select(oldVertices(ID), graph.degrees("degree"))
        .na.fill(0)

    val lowDegreeVertexIDs = vertexDegrees.filter(vertexDegrees("degree") <= maxDegree).select(ID)


    val inducedVertices = oldVertices.join(lowDegreeVertexIDs, lowDegreeVertexIDs(ID) === oldVertices(ID))
        .drop(lowDegreeVertexIDs(ID))
      .select(oldVertices.columns.map(col) :_*)

     new UndirectedGraph(GraphFrameUtilities.vertexInducedSubgraph(graph, lowDegreeVertexIDs).cache(), outName)
  }
}
