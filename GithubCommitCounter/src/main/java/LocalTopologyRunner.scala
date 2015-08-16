import backtype.storm.utils.Utils
import backtype.storm.{LocalCluster, Config}
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields

object LocalTopologyRunner {

  private val tenMinutes = 600000

  def main (args: Array[String]) {
    val builder = new TopologyBuilder()

    builder.setSpout("commit-feed-listener", new CommitFeedListener())

    builder.setBolt("email-extractor", new EmailExtractor())
      .shuffleGrouping("commit-feed-listener")

    builder.setBolt("email-counter", new EmailCounter())
      .fieldsGrouping("email-extractor", new Fields("email"))

    val config = new Config()
    config.setDebug(true)

    val topology = builder.createTopology()

    val cluster = new LocalCluster()
    cluster.submitTopology("github-commit-count-topology", config, topology)

    Utils.sleep(tenMinutes)
    cluster.killTopology("github-commit-count-topology")
    cluster.shutdown()
  }

}
