import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields

object HeatmapTopologyBuilder {
  def build(): StormTopology = {
    val builder = new TopologyBuilder

    builder.setSpout("checkins", new Checkins, 4)
    builder.setBolt("geocode-lookup", new GeocodeLookup, 8)
      .setNumTasks(64)
      .shuffleGrouping("checkins")
    builder.setBolt("time-interval-extractor", new TimeIntervalExtractor)
      .shuffleGrouping("geocode-lookup")
    builder.setBolt("heatmap-builder", new HeatMapBuilder)
      .fieldsGrouping("time-interval-extractor", new Fields("time-interval"))
    builder.setBolt("persistor", new Persistor)
      .shuffleGrouping("heatmap-builder")

    builder.createTopology()
  }
}
