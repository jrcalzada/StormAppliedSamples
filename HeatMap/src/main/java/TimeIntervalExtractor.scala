import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}

class TimeIntervalExtractor extends BaseBasicBolt {
  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val interval = tuple.getLongByField("time") / (15 * 1000)
    val geocode = tuple.getValueByField("geocode")
    basicOutputCollector.emit(new Values(new java.lang.Long(interval), geocode))
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("time-interval", "geocode"))
  }
}
