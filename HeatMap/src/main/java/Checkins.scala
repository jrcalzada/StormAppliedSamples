import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Values, Fields}

class Checkins extends BaseRichSpout {

  private var output: SpoutOutputCollector = null
  private var nextCheckinToEmit: Int = 0
  private val checkinData = List[String](
    "1382904793783, 287 Hudson St New York NY 10013",
    "1382904793784, 155 Varick St New York NY 10013",
    "1382904793785, 222 W Houston St New York NY 10013",
    "1382904793786, 5 Spring St New York NY 10013",
    "1382904793787, 148 West 4th St New York NY 10013"
  )

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("time", "address"))
  }

  override def nextTuple(): Unit = {
    val checkin = checkinData(nextCheckinToEmit)
    val parts = checkin.split(", ")
    val time = parts(0).toLong
    val address = parts(1)
    output.emit(new Values(new java.lang.Long(System.currentTimeMillis()), address))

    Thread.sleep(5000)
    nextCheckinToEmit = (nextCheckinToEmit + 1) % checkinData.size
  }

  override def open(map: util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector): Unit = {
    output = spoutOutputCollector
    nextCheckinToEmit = 0
  }
}
