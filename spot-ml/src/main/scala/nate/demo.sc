import nate._
import nate.Time.time
import org.apache.spark.sql.functions._

// this is meant to be executed in a spark shell so we can get the sql context

// we also need some external flow data... here's some on Las Vegas:

val path = "/user/duxbury/flow/hive/y=2016/m=04/d=23/h=02"

// be warned:  if you try the full day, the clustering coefficient calculation below will choke
//  the reason why will be quite apparent when you see the degree distribution of the projected_trimmed graph for the
//   full day

val externalFlows = new CrossPerimeterFlowSet(sqlContext, path )

val externalFlowGraph = externalFlows.externalToInternalFlowGraph("externalFlowGraph")

time {   externalFlowGraph.summarize() }



val trimmed = externalFlowGraph.dropHighDegreeNodes(512, 512, "externalFlowGraph_trimmed")


time { trimmed.summarize() }


val projected_trimmed = trimmed.projectOutSinks()
time {projected_trimmed .summarize() }



val cc = projected_trimmed.clusteringCoefficient
time {   cc.show(10) }





cc.describe().show()

cc.sort("clustering_coefficient").show(50)

cc.sort(desc("clustering_coefficient").show(50)


