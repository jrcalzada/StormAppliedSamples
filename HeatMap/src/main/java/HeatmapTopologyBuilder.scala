import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder

object HeatmapTopologyBuilder {
  def build(): StormTopology = {
    val builder = new TopologyBuilder

    builder.setSpout("checkins", new Checkins)
    builder.setBolt("geocode-lookup", new GeocodeLookup)
      .shuffleGrouping("checkins")
    builder.setBolt("heatmap-builder", new HeatMapBuilder)
      .globalGrouping("geocode-lookup")
    builder.setBolt("persistor", new Persistor)
      .shuffleGrouping("heatmap-builder")

    builder.createTopology()
  }
}
