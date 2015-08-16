import java.util

import backtype.storm.task.TopologyContext
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Tuple
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.code.geocoder.model.LatLng
import redis.clients.jedis.Jedis

class Persistor extends BaseBasicBolt {
  private val objectMapper = new ObjectMapper
  private var jedis: Jedis = null

  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val interval = tuple.getLongByField("interval")
    val hotzones = tuple.getValueByField("hotzones").asInstanceOf[List[LatLng]]
        .foldLeft("[")((str, element) => str + "{" + element.toUrlValue + "}, ")
        .dropRight(2) + "]"

    val key = "checkins-" + interval
    val value = hotzones
    jedis.set(key, value)
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {

  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext): Unit = {
    jedis = new Jedis("localhost")
  }

  override def cleanup(): Unit = {
    if (jedis.isConnected) {
      jedis.quit()
    }
  }
}
