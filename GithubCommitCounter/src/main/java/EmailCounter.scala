import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Tuple

import scala.collection.mutable

class EmailCounter extends BaseBasicBolt {
  private val emailCounts = new mutable.HashMap[String, Int]()

  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val email = tuple.getStringByField("email")
    emailCounts(email) = countFor(email) + 1
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = { }

  private def countFor(email: String): Int = {
    val count = emailCounts.get(email)
    if (count.isEmpty) 0 else count.get
  }
}
