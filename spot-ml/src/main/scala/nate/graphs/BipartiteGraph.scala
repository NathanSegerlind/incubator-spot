package nate.graphs

import nate.GraphFrameUtilities
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame
import nate.Schema._
import org.apache.spark.sql.functions._

import scala.math.pow

/**
  * Represenation of bipartite graph, with no edge weights or multiplicities.
  *
  * It just wraps a graph frame and provides some methods for interpreting it as a bipartite graph.
  *
  * @param graph graph frame
  * @param name
  */
class BipartiteGraph(graph: GraphFrame, name : String = "bipartite graph") extends DirectedGraph(graph, name) {


  lazy val isolatedVertices = GraphFrameUtilities.getIsolatedVertices(graph).cache()
  lazy val sources = graph.edges.select(SRC).toDF(ID).distinct().cache()
  lazy val sinks   = graph.edges.select(DST).toDF(ID).distinct().cache()



  def projectOutSources(projectedName: String = name + "_projectOutSources") : UndirectedGraph = {

    val otherSRC = "other_" + SRC
    val otherDST = "other_" + DST

    val other = edges.select(SRC,DST).toDF(otherSRC, otherDST)


    val e = edges.join(other, edges(SRC)=== other(otherSRC) && (edges(DST) !== other(otherDST)))
      .select(DST,otherDST).toDF(SRC, DST).distinct()

    new UndirectedGraph(GraphFrame.fromEdges(e).cache(), projectedName)
  }

  def projectOutSinks(projectedName: String = name + "_projectOutSinks") : UndirectedGraph = {

    val otherSRC = "other_" + SRC
    val otherDST = "other_" + DST

    val other = edges.select(SRC,DST).toDF(otherSRC, otherDST)


    val directedEdges = edges.join(other, edges(DST)=== other(otherDST) && (edges(SRC) !== other(otherSRC)))
      .select(SRC,otherSRC).toDF(SRC, DST)


    val undirectedEdges  =  directedEdges.filter(s"$SRC != $DST")
      .selectExpr(s"if($SRC < $DST, $SRC, $DST) as $SRC",
        s"if($SRC < $DST, $DST, $SRC) as $DST")
      .dropDuplicates(Seq(SRC, DST))

    new UndirectedGraph(GraphFrame.fromEdges(undirectedEdges).cache(), projectedName)
  }

  override def summarize() : Unit = {


    val vCount = graph.vertices.count()
    val sourceCount = sources.count()
    val sinkCount  = sinks.count()
    val eCount = graph.edges.count()
    val maxEdges = sourceCount * sinkCount

    println("Bipartite graph: " + name)
    println("vertex count:  " + vCount)

    println("source-vertex count: " + sourceCount)
    println("sink-vertex count: " + sinkCount)
    println("edge count:  " + eCount + "  out of a possible " +  maxEdges)

    println("edge density:   " +  eCount.toDouble / maxEdges.toDouble)

    val outDegrees : DataFrame = graph.outDegrees.cache()

    outDegrees.registerTempTable("outDegrees")



    val maxOutDegreeDF = sqlContext.sql("SELECT MAX(outDegree) as maxdegree FROM outDegrees")
    val maxOutDegree = maxOutDegreeDF.take(1)(0).getInt(0)
    println("MAXIMUM OUT-DEGREE: " + maxOutDegree)

    val ceilLog2MaxOutDegree = Math.ceil(scala.math.log(maxOutDegree) / scala.math.log(2)).toInt

    def expOutDegreeBucket(df: DataFrame, i: Int) = df.filter("outDegree >= " + pow(2, i)).filter("outDegree < " + pow(2, i + 1))
    Range(0, ceilLog2MaxOutDegree).toList.foreach(i => {
      val c = expOutDegreeBucket(outDegrees, i).count()
      println("vertex count with out-degree in range  " + pow(2, i) + " <= out-degree < " + pow(2, i + 1) + ": " + c)
    })

    val inDegrees : DataFrame = graph.inDegrees.cache()

    inDegrees.registerTempTable("inDegrees")

    val maxInDegreeDF = sqlContext.sql("SELECT MAX(inDegree) as maxdegree FROM inDegrees")
    val maxInDegree = maxInDegreeDF.take(1)(0).getInt(0)
    println("MAXIMUM IN-DEGREE: " + maxInDegree)

    val ceilLog2MaxInDegree = Math.ceil(scala.math.log(maxInDegree) / scala.math.log(2)).toInt

    def expInDegreeBucket(df: DataFrame, i: Int) = df.filter("inDegree >= " + pow(2, i)).filter("inDegree < " + pow(2, i + 1))
    Range(0, ceilLog2MaxInDegree).toList.foreach(i => {
      val c = expInDegreeBucket(inDegrees, i).count()
      println("vertex count with in-degree in range  " + pow(2, i) + " <= in-degree < " + pow(2, i + 1) + ": " + c)
    })
  }

  override def induceOnVertices(iVertices: DataFrame, inducedName : String = name + " (vertex induced)") : BipartiteGraph =
  {
    new BipartiteGraph(GraphFrameUtilities.vertexInducedSubgraph(graph, iVertices).cache(), inducedName)
  }



  def dropHighDegreeNodes(maxSourceDegree: Long, maxSinkDegree: Long, outName: String = name + "_degreefiltered"): BipartiteGraph = {
    val oldEdges = graph.edges



    val sinkInDegrees = sinks.join(graph.inDegrees, sinks(ID) === graph.inDegrees(ID))
      .select(sinks(ID), graph.inDegrees("inDegree"))

    val lowDegreeSinkIDs = sinkInDegrees.filter(sinkInDegrees("inDegree") <= maxSinkDegree).select(ID)


    val sourceOutDegrees = sources.join(graph.outDegrees, sources(ID) === graph.outDegrees(ID))
      .select(sources(ID), graph.outDegrees("outDegree"))


    val lowDegreeSourceIDs = sourceOutDegrees.filter(sourceOutDegrees("outDegree") <= maxSourceDegree).select(ID)

    val isolatedVertexIDs = isolatedVertices.select(ID)

    val lowDegreeVertexIDs = lowDegreeSinkIDs.unionAll(isolatedVertexIDs).unionAll(lowDegreeSourceIDs)


    new BipartiteGraph(GraphFrameUtilities.vertexInducedSubgraph(graph, lowDegreeVertexIDs).cache(), outName)
  }

}
