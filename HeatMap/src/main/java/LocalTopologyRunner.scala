import backtype.storm.{Config, LocalCluster}

object LocalTopologyRunner {

  def main (args: Array[String]) {
    val config = new Config
    val topology = HeatmapTopologyBuilder.build()

    val localCluster = new LocalCluster()
    localCluster.submitTopology("heatmap-local", config.asInstanceOf[java.util.Map[_, _]], topology)
  }

}
