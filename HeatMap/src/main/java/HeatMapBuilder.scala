import backtype.storm.{Constants, Config}
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Fields, Values, Tuple}
import com.google.code.geocoder.model.LatLng

import scala.collection.mutable

class HeatMapBuilder extends BaseBasicBolt {
  private val heatmaps = new mutable.HashMap[Long, List[LatLng]]

  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    if (isTickTuple(tuple)) {
      emitHeatmap(basicOutputCollector)
    } else {
      val timeInterval = tuple.getLongByField("time-interval")
      val latLng = tuple.getValueByField("geocode").asInstanceOf[LatLng]
      heatmaps(timeInterval) = latLng :: heatmaps.getOrElse(timeInterval, List[LatLng]())
    }
  }

  private def selectTimeInterval(time: Long) = time / (15 * 1000)

  private def isTickTuple(tuple: Tuple) = {
    val sourceComponent = tuple.getSourceComponent
    val sourceStreamId = tuple.getSourceStreamId
    sourceComponent == Constants.SYSTEM_COMPONENT_ID && sourceStreamId == Constants.SYSTEM_TICK_STREAM_ID
  }

  private def emitHeatmap(output: BasicOutputCollector): Unit = {
    val now = System.currentTimeMillis()
    val emitUpTo = selectTimeInterval(now)
    val intervals = heatmaps.keySet
    for (interval <- intervals) {
      if (interval < emitUpTo) {
        val hotzones = heatmaps.remove(interval)
        output.emit(new Values(new java.lang.Long(interval), hotzones.get))
      }
    }
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("interval", "hotzones"))
  }

  override def getComponentConfiguration: java.util.Map[String, AnyRef] = {
    val config = new Config
    config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, new Integer(60))
    config
  }
}
