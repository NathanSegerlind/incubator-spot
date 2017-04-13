package nate

import nate.Schema._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object GraphFrameUtilities {


  def vertexInducedSubgraph(graph: GraphFrame, keeperVertices: DataFrame) : GraphFrame = {

    val idsOfKeeperVertices = keeperVertices.select(ID).cache()
    val oldVertices = graph.vertices

    val inducedVertices =
      oldVertices.join(idsOfKeeperVertices, idsOfKeeperVertices(ID) === oldVertices(ID))
        .drop(idsOfKeeperVertices(ID)).select(oldVertices.columns.map(col) :_*)


    val oldEdges = graph.edges

    val inducedEdges = oldEdges
      .join(idsOfKeeperVertices,  idsOfKeeperVertices(ID) === oldEdges(SRC))
      .select(oldEdges.columns.map(col) :_*)
      .join(idsOfKeeperVertices,  idsOfKeeperVertices(ID) === oldEdges(DST))
      .select(oldEdges.columns.map(col) :_*)


    GraphFrame(inducedVertices, inducedEdges)
  }

  def getIsolatedVertices(graph: GraphFrame) : DataFrame = {

    val vertexDegrees = graph.vertices.join(graph.degrees, graph.vertices(ID) === graph.degrees(ID), "left_outer")
      .select(graph.vertices(ID), graph.degrees("degree"))
      .na.fill(0d)

    val isolatedVertexIDs = vertexDegrees
      .filter(vertexDegrees("degree") === 0d)
      .drop(vertexDegrees("degree")).select(vertexDegrees(ID))

    graph.vertices.join(isolatedVertexIDs, isolatedVertexIDs(ID) === graph.vertices(ID))
      .drop(isolatedVertexIDs(ID)).select(graph.vertices.columns.map(col) :_*)
  }

}
