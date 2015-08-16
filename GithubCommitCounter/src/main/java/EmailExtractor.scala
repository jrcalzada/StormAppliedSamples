import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}

class EmailExtractor extends BaseBasicBolt {
  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val commit = tuple.getStringByField("commit")
    val parts = commit.split(" ")
    basicOutputCollector.emit(new Values(parts(1)))
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("email"))
  }
}
