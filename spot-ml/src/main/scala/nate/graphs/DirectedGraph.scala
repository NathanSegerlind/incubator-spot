package nate.graphs

import nate.GraphFrameUtilities
import nate.Schema._
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame

import scala.math.pow

/**
  * Represenation of directed graph, posisbly with loops, but no edge weights or multiplicities.
  *
  * It just wraps a graph frame and provides some methods for interpreting it as a directed graph.
  *
  * @param graph graph frame
  * @param name
  */
class DirectedGraph(graph: GraphFrame,name : String = "directed graph") extends Graph[DirectedGraph](graph, name) {



  def summarize() : Unit = {


    val vCount = graph.vertices.count()
    val eCount = graph.edges.count()

    println("Directed Graph: " + name)
    println("vertex count:  " + vCount)
    val maxEdges = vCount * vCount
    println("edge count:  " + eCount + "  out of a possible " + maxEdges)
    println("edge density:   " +  eCount.toDouble / maxEdges.toDouble)

    val outDegrees : DataFrame = graph.outDegrees.cache()

    outDegrees.registerTempTable("outDegrees")



    val maxDegreeDF = sqlContext.sql("SELECT MAX(outDegree) as maxdegree FROM outDegrees")
    val maxOutDegree = maxDegreeDF.take(1)(0).getInt(0)
    println("MAXIMUM OUT DEGREE: " + maxOutDegree)

    val ceilLog2MaxOutDegree = Math.ceil(scala.math.log(maxOutDegree) / scala.math.log(2)).toInt

    def expOutDegreeBucket(df: DataFrame, i: Int) = df.filter("outDegree >= " + pow(2, i)).filter("outDegree < " + pow(2, i + 1))
    Range(0, ceilLog2MaxOutDegree).toList.foreach(i => {
      val c = expOutDegreeBucket(outDegrees, i).count()
      println("vertex count with out-degree in range  " + pow(2, i) + " <= out-degree < " + pow(2, i + 1) + ": " + c)
    })

    val inDegrees : DataFrame = graph.inDegrees.cache()

    inDegrees.registerTempTable("inDegrees")

    println("MAXIMUM IN DEGREE (of internal IP)")

    val maxInDegreeDF = sqlContext.sql("SELECT MAX(inDegree) as maxdegree FROM inDegrees")
    val maxInDegree = maxInDegreeDF.take(1)(0).getInt(0)
    println("MAXIMUM IN DEGREE: " + maxInDegree)

    val ceilLog2MaxInDegree = Math.ceil(scala.math.log(maxInDegree) / scala.math.log(2)).toInt

    def expInDegreeBucket(df: DataFrame, i: Int) = df.filter("inDegree >= " + pow(2, i)).filter("inDegree < " + pow(2, i + 1))
    Range(0, ceilLog2MaxInDegree).toList.foreach(i => {
      val c = expInDegreeBucket(inDegrees, i).count()
      println("vertex count with in-degree in range  "+ pow(2, i) + " <= in-degree < " + pow(2, i + 1) + ": " + c)
    })
  }



  def induceOnVertices(iVertices: DataFrame, inducedName : String = name + " (vertex induced)") : DirectedGraph =  {
    new DirectedGraph(GraphFrameUtilities.vertexInducedSubgraph(graph, iVertices).cache(), inducedName)
  }

  def toUndirectedGraph(undirectedName : String = name + "_undirected") =  {
    val undirectedEdges = graph.edges.filter(s"$SRC != $DST")
      .selectExpr(s"if($SRC < $DST, $SRC, $DST) as $SRC",
        s"if($SRC < $DST, $DST, $SRC) as $DST")
      .dropDuplicates(Seq(SRC, DST))

    new UndirectedGraph(GraphFrame.fromEdges(undirectedEdges).cache(), name)
  }
}
