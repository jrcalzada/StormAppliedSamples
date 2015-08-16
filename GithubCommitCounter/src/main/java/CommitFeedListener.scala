import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{Values, Fields}

class CommitFeedListener extends BaseRichSpout {

  private var output: SpoutOutputCollector = null
  private val commits = List[String](
    "b20ea50 nathan@example.com",
    "064874b andy@example.com",
    "28e4f8e andy@example.com",
    "9a3e07f andy@example.com",
    "cbb9cd1 nathan@example.com",
    "0f663d2 jackson@example.com",
    "0a4b984 nathan@example.com",
    "1915ca4 derek@example.com"
  )

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("commit"))
  }

  override def nextTuple(): Unit = {
    for (commit <- commits) {
      output.emit(new Values(commit))
    }
  }

  override def open(map: util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector): Unit = {
    output = spoutOutputCollector
  }
}
