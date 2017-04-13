package nate.graphs

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spot.testutils.TestingSparkContextFlatSpec
import org.graphframes.GraphFrame
import org.scalatest.Matchers


class BipartiteGraphTest extends TestingSparkContextFlatSpec with Matchers {





  "vertex induced subgraph" should "be empty if we induce to disjoint vertex set" in {
    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("1", "Eddie"),
      ("2", "Figaro"),
      ("3", "Gustav"),
      ("4", "Herodotus")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "1"),
      ("a", "2"),
      ("a", "3"),
      ("b", "1"),
      ("b", "2"),
      ("c", "3")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)
    val bipartiteGraph = new BipartiteGraph(g, "undirected_empty_vertex_restriction_test")

    val keepVerts = sqlContext.createDataFrame(List(Tuple1("h"), Tuple1("i"), Tuple1("12"))).toDF("id")

    val result = bipartiteGraph.induceOnVertices(keepVerts)

    result.vertices.collect().length shouldBe 0
    result.edges.collect().length   shouldBe 0
  }

  "vertex induced subgraph" should "be exactly the same if we induce to superset of the vertex set" in {

    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("1", "Eddie"),
      ("2", "Figaro"),
      ("3", "Gustav"),
      ("4", "Herodotus")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "1"),
      ("a", "2"),
      ("a", "3"),
      ("b", "1"),
      ("b", "2"),
      ("c", "3")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)

    val bipartiteGraph = new BipartiteGraph(g, "superset_vertex_restriction_test")
    val keepVerts = sqlContext.createDataFrame(List(Tuple1("a"), Tuple1("b"), Tuple1("c"), Tuple1("d"), Tuple1("1"),
      Tuple1("2"), Tuple1("3"), Tuple1("4"))).toDF("id")

    val result = bipartiteGraph.induceOnVertices(keepVerts)

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
      ("1", "Eddie"),
      ("2", "Figaro"),
      ("3", "Gustav"),
      ("4", "Herodotus")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "1"),
      ("a", "2"),
      ("a", "3"),
      ("b", "1"),
      ("b", "2"),
      ("c", "3")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)

    val bipartiteGraph = new BipartiteGraph(g, "nonempty_strictsubset_vertex_restriction_test")
    val keepVerts = sqlContext.createDataFrame(List(Tuple1("b"), Tuple1("c"),  Tuple1("d"), Tuple1("2"), Tuple1("3"))).toDF("id")

    val result = bipartiteGraph.induceOnVertices(keepVerts)

    val expectedV = sqlContext.createDataFrame(List(
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("2", "Figaro"),
      ("3", "Gustav")
    )).toDF("id", "name")

    val expectedE = sqlContext.createDataFrame(List(
      ("b", "2"),
      ("c", "3")
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
      ("1", "Eddie"),
      ("2", "Figaro"),
      ("3", "Gustav"),
      ("4", "Herodotus")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "1"),
      ("a", "2"),
      ("a", "3"),
      ("b", "1"),
      ("b", "2"),
      ("c", "3")
    )).toDF("src", "dst")



    // Create a GraphFrame

    val g = GraphFrame(v, e)
    val bipartiteGraph = new BipartiteGraph(g, "drop_high_degree_nodes_test_threshold_>=_max_degree")

    val result = bipartiteGraph.dropHighDegreeNodes(3,2)


    val expectedV = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("1", "Eddie"),
      ("2", "Figaro"),
      ("3", "Gustav"),
      ("4", "Herodotus")
    )).toDF("id", "name")

    val expectedE = sqlContext.createDataFrame(List(
      ("a", "1"),
      ("a", "2"),
      ("a", "3"),
      ("b", "1"),
      ("b", "2"),
      ("c", "3")
    )).toDF("src", "dst")

    val expectedG  = GraphFrame(expectedV, expectedE)

    result.vertices.schema shouldEqual expectedG.vertices.schema // can't let it lose any vertex data !
    result.edges.schema shouldEqual expectedG.edges.schema
    result.vertices.collect().toSet  shouldEqual expectedG.vertices.collect().toSet
    result.edges.collect().toSet shouldEqual expectedG.edges.collect().toSet
  }



  "drop high degree nodes" should "drop the source vertex of degree > the source threshold" in {

    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("1", "Eddie"),
      ("2", "Figaro"),
      ("3", "Gustav"),
      ("4", "Herodotus")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "1"),
      ("a", "2"),
      ("a", "3"),
      ("b", "1"),
      ("b", "2"),
      ("c", "3")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)
    val bipartiteGraph = new BipartiteGraph(g, "drop_high_degree_nodes_test")
    val result = bipartiteGraph.dropHighDegreeNodes(2, 2)


    val expectedV = sqlContext.createDataFrame(List(
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("1", "Eddie"),
      ("2", "Figaro"),
      ("3", "Gustav"),
      ("4", "Herodotus")
    )).toDF("id", "name")

    val expectedE = sqlContext.createDataFrame(List(

      ("b", "1"),
      ("b", "2"),
      ("c", "3")
    )).toDF("src", "dst")

    val expectedG  = GraphFrame(expectedV, expectedE)

    result.vertices.schema shouldEqual expectedG.vertices.schema
    result.edges.schema shouldEqual expectedG.edges.schema
    result.vertices.collect().toSet  shouldEqual expectedG.vertices.collect().toSet
    result.edges.collect().toSet shouldEqual expectedG.edges.collect().toSet
  }


  "project out sinks" should "work as expected" in {
    val v = sqlContext.createDataFrame(List(
      ("a", "Alice"),
      ("b", "Bob"),
      ("c", "Charlie"),
      ("d", "Doris"),
      ("1", "Eddie"),
      ("2", "Figaro"),
      ("3", "Gustav"),
      ("4", "Herodotus")
    )).toDF("id", "name")

    val e = sqlContext.createDataFrame(List(
      ("a", "1"),
      ("a", "2"),
      ("a", "3"),
      ("b", "1"),
      ("b", "2"),
      ("c", "3")
    )).toDF("src", "dst")


    // Create a GraphFrame

    val g = GraphFrame(v, e)
    val bipartiteGraph = new BipartiteGraph(g, "project_out_sinks_test")
    val result = bipartiteGraph.projectOutSinks()


    val expectedE = sqlContext.createDataFrame(List(

      ("a", "b"),
      ("a", "c")
    )).toDF("src", "dst")

    val expectedG  = GraphFrame.fromEdges(expectedE)

    result.vertices.collect().toSet  shouldEqual expectedG.vertices.collect().toSet
    result.edges.collect().toSet shouldEqual expectedG.edges.collect().toSet
  }
}
