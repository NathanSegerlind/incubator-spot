package nate.graphs

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spot.testutils.TestingSparkContextFlatSpec
import org.graphframes.GraphFrame
import org.scalatest.Matchers


class UndirectedGraphTest extends TestingSparkContextFlatSpec with Matchers {





  "vertex induced subgraph" should "be empty if we induce to disjoint vertex set" in {
    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("e", "Eddie"),
      ("f", "Figaro"),
      ("g", "Gustav")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("a", "c"),
      ("b", "c"),
      ("b", "e"),
      ("b", "f"),
      ("c", "d")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)
    val undirectedGraph = new UndirectedGraph(g, "undirected_empty_vertex_restriction_test")

    val keepVerts = sqlContext.createDataFrame(List(Tuple1("h"), Tuple1("i"), Tuple1("j"))).toDF("id")

    val result = undirectedGraph.induceOnVertices(keepVerts)

    result.vertices.collect().length shouldBe 0
    result.edges.collect().length   shouldBe 0
  }

  "vertex induced subgraph" should "be exactly the same if we induce to superset of the vertex set" in {

    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("e", "Eddie"),
      ("f", "Figaro"),
      ("g", "Gustav")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("a", "c"),
      ("b", "c"),
      ("b", "e"),
      ("b", "f"),
      ("c", "d")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)

    val undirectedGraph = new UndirectedGraph(g, "superset_vertex_restriction_test")
    val keepVerts = sqlContext.createDataFrame(List(Tuple1("a"), Tuple1("b"), Tuple1("c"), Tuple1("d"), Tuple1("e"),
      Tuple1("f"), Tuple1("g"), Tuple1("h"), Tuple1("i"), Tuple1("j"))).toDF("id")

    val result = undirectedGraph.induceOnVertices(keepVerts)

    result.vertices.schema shouldEqual g.vertices.schema
    result.edges.schema shouldEqual g.edges.schema
    result.vertices.collect().toSet  shouldEqual g.vertices.collect().toSet
    result.edges.collect().toSet shouldEqual g.edges.collect().toSet
  }



  "vertex induced subgraph" should "calculate non-empty vertex induced subgraph" in {

    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("e", "Eddie"),
      ("f", "Figaro"),
      ("g", "Gustav")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("a", "c"),
      ("b", "c"),
      ("b", "e"),
      ("b", "f"),
      ("c", "d")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)

    val undirectedGraph = new UndirectedGraph(g, "nonempty_strictsubset_vertex_restriction_test")
    val keepVerts = sqlContext.createDataFrame(List(Tuple1("a"), Tuple1("b"),  Tuple1("f"), Tuple1("g"), Tuple1("j"))).toDF("id")

    val result = undirectedGraph.induceOnVertices(keepVerts)

    val expectedV = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("f", "Figaro"),
      ("g", "Gustav")
    )).toDF("id", "name")

    val expectedE = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("b", "f")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val expectedG = GraphFrame(expectedV, expectedE)


    result.vertices.schema shouldEqual expectedG.vertices.schema
    result.edges.schema shouldEqual expectedG.edges.schema
    result.vertices.collect().toSet  shouldEqual expectedG.vertices.collect().toSet
    result.edges.collect().toSet shouldEqual expectedG.edges.collect().toSet
  }


  "drop high degree nodes" should "be exactly the same if we set the limit at max degree" in {

    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("e", "Eddie"),
      ("f", "Figaro"),
      ("g", "Gustav")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("a", "c"),
      ("b", "c"),
      ("b", "e"),
      ("b", "f"),
      ("c", "d")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)
    val undirectedGraph = new UndirectedGraph(g, "drop_high_degree_nodes_test_threshold_>=_max_degree")

    val result = undirectedGraph.dropHighDegreeNodes(4)


    val expectedV = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("e", "Eddie"),
      ("f", "Figaro"),
      ("g", "Gustav")
    )).toDF("id", "name")

    val expectedE = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("a", "c"),
      ("b", "c"),
      ("b", "e"),
      ("b", "f"),
      ("c", "d")
    )).toDF("src", "dst")

    val expectedG  = GraphFrame(expectedV, expectedE)

    result.vertices.schema shouldEqual expectedG.vertices.schema // can't let it lose any vertex data !
    result.edges.schema shouldEqual expectedG.edges.schema
    result.vertices.collect().toSet  shouldEqual expectedG.vertices.collect().toSet
    result.edges.collect().toSet shouldEqual expectedG.edges.collect().toSet
  }



  "drop high degree nodes" should "drop the vertex of degree > the threshold" in {

    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("e", "Eddie"),
      ("f", "Figaro"),
      ("g", "Gustav")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("a", "c"),
      ("b", "a"),
      ("b", "c"),
      ("b", "e"),
      ("b", "f"),
      ("c", "d")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)
    val undirectedGraph = new UndirectedGraph(g, "drop_high_degree_nodes_test")
    val result = undirectedGraph.dropHighDegreeNodes(3)


    val expectedV = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("e", "Eddie"),
      ("f", "Figaro"),
      ("g", "Gustav")
    )).toDF("id", "name")

    val expectedE = sqlContext.createDataFrame(List(
      ("a", "c"),
      ("c", "d")
    )).toDF("src", "dst")

    val expectedG  = GraphFrame(expectedV, expectedE)

    result.vertices.schema shouldEqual expectedG.vertices.schema
    result.edges.schema shouldEqual expectedG.edges.schema
    result.vertices.collect().toSet  shouldEqual expectedG.vertices.collect().toSet
    result.edges.collect().toSet shouldEqual expectedG.edges.collect().toSet
  }

  "clustering coefficient" should "correctly handle single edge" in {


    val e = sqlContext.createDataFrame(List(
      ("a", "b")
    )).toDF("src", "dst")

    val g = GraphFrame.fromEdges(e)
    val undirectedGraph = new UndirectedGraph(g, "drop_high_degree_nodes_test")
    val result : DataFrame = undirectedGraph.clusteringCoefficient()

    val expectedValues = Set( ("a", 1d), ("b", 1d))

    result.columns.toSet shouldEqual Set("id", "clustering_coefficient")
    result.collect().map(row => (row.getString(0), row.getDouble(1))).toSet shouldEqual expectedValues
  }

  "clustering coefficient" should "correctly a line of three vertices" in {


    val e = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("b", "c")
    )).toDF("src", "dst")

    val g = GraphFrame.fromEdges(e)
    val undirectedGraph = new UndirectedGraph(g, "drop_high_degree_nodes_test")

    val result : DataFrame = undirectedGraph.clusteringCoefficient

    val expectedValues = Set( ("a", 1d), ("b", 0d),  ("c", 1d))

    result.columns.toSet shouldEqual Set("id", "clustering_coefficient")
    result.collect().map(row => (row.getString(0), row.getDouble(1))).toSet shouldEqual expectedValues
  }
  "clustering coefficient" should "correctly handle adjacent triangles" in {


    val e = sqlContext.createDataFrame(List(
      ("a", "b"),
      ("a", "c"),
      ("a", "d"),
      ("b", "c"),
      ("b", "d")
    )).toDF("src", "dst")

    val g = GraphFrame.fromEdges(e)
    val undirectedGraph = new UndirectedGraph(g, "drop_high_degree_nodes_test")

    val result : DataFrame = undirectedGraph.clusteringCoefficient

    val expectedValues = Set( ("a", 2d/3d), ("b", 2d/3d), ("c", 1d), ("d", 1d))

    result.columns.toSet shouldEqual Set("id", "clustering_coefficient")
    result.collect().map(row => (row.getString(0), row.getDouble(1))).toSet shouldEqual expectedValues
  }
}
