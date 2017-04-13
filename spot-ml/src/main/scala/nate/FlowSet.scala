package nate

import java.lang.Math.pow

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spot.netflow.FlowSchema._
import org.graphframes.GraphFrame
import nate.Schema._
import nate.graphs.{BipartiteGraph, DirectedGraph, UndirectedGraph}


abstract class FlowSet(val sqlContext: SQLContext, val path: String) {
  val flows : DataFrame

  protected lazy val directedEdges :  DataFrame = {
    flows.select(SourceIP, DestinationIP).toDF(Seq(SRC,DST) :_*).distinct().cache()
  }

  def toDirectedFlowGraph(name: String = path) : DirectedGraph = {
    new DirectedGraph(GraphFrame.fromEdges(directedEdges).cache(), name)
  }


  def toUndirectedFlowGraph(name: String = path) : UndirectedGraph = {


    val undirectedEdges  =  directedEdges.filter(s"$SRC != $DST")
      .selectExpr(s"if($SRC < $DST, $SRC, $DST) as $SRC",
        s"if($SRC < $DST, $DST, $SRC) as $DST")
      .dropDuplicates(Seq(SRC, DST))


    new UndirectedGraph(GraphFrame.fromEdges(undirectedEdges).cache(), name)
  }

}

class InternalFlowSet(sqlContext: SQLContext,path: String) extends FlowSet(sqlContext, path) {


  lazy val flows = {
    val internalLinkUDF = udf((src: String, dst: String) => (IPUtils.isInternalIP(src) && IPUtils.isInternalIP(dst)))

    val cleanAllFlows = sqlContext.read.parquet(path).select(SourceIP, DestinationIP)
      .filter(SourceIP + " is not null").filter(DestinationIP + " is not null")
      .filter(SourceIP + " != ''").filter(DestinationIP + " != ''")
      .filter(SourceIP + " != 'sa'").filter(DestinationIP + " != 'da'")

    val internalLabeledFlows = cleanAllFlows
      .withColumn("srcAndDstInternal", internalLinkUDF(cleanAllFlows(SourceIP), cleanAllFlows(DestinationIP)))

    internalLabeledFlows.filter(internalLabeledFlows("srcAndDstInternal") === true).select(SourceIP, DestinationIP)
      .cache()
  }
}


class CrossPerimeterFlowSet(sqlContext: SQLContext, path: String) extends FlowSet(sqlContext, path) {


  lazy val flows = {


    val cleanAllFlows = sqlContext.read.parquet(path).select(SourceIP, DestinationIP)
      .filter(SourceIP + " is not null").filter(DestinationIP + " is not null")
      .filter(SourceIP + " != ''").filter(DestinationIP + " != ''")
      .filter(SourceIP + " != 'sa'").filter(DestinationIP + " != 'da'")

    val crossPerimeterUDF = udf((src: String, dst: String) => ((IPUtils.isInternalIP(src) && IPUtils.isExternalIP(dst))
      || (IPUtils.isExternalIP(src) && IPUtils.isInternalIP(dst))))

    val crossPerimeterLabeledFlows = cleanAllFlows
      .withColumn("crossPerimeter", crossPerimeterUDF(cleanAllFlows(SourceIP), cleanAllFlows(DestinationIP)))

    crossPerimeterLabeledFlows.filter(crossPerimeterLabeledFlows("crossPerimeter") === true).select(SourceIP, DestinationIP)
        .cache()
  }

  protected lazy val extToIntEdges :  DataFrame = {
    val extUDF = udf((src: String, dst: String) =>  if (IPUtils.isExternalIP(src)) src else dst)
    val intUDF = udf((src: String, dst: String) =>  if (IPUtils.isInternalIP(src)) src else dst)

    directedEdges.withColumn("ext", extUDF(directedEdges(SRC), directedEdges(DST)))
      .withColumn("int", intUDF(directedEdges(SRC), directedEdges(DST)))
        .select("ext", "int").toDF(SRC,DST)
      .dropDuplicates
  }

  directedEdges.filter(s"$SRC != $DST")

  def externalToInternalFlowGraph(name: String = path) : BipartiteGraph = {
    new BipartiteGraph(GraphFrame.fromEdges(extToIntEdges).cache(), name)
  }



}
