import java.util

import backtype.storm.task.TopologyContext
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}
import com.google.code.geocoder.model.GeocoderStatus
import com.google.code.geocoder.{GeocoderRequestBuilder, Geocoder}

class GeocodeLookup extends BaseBasicBolt {
  private var geocoder: Geocoder = null

  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val address = tuple.getStringByField("address")
    val time = tuple.getLongByField("time")

    val geocodeBuilder = new GeocoderRequestBuilder()
      .setAddress(address)
      .setLanguage("en")
    val request = geocodeBuilder.getGeocoderRequest
    val response = geocoder.geocode(request)

    if (response.getStatus == GeocoderStatus.OK) {
      val result = response.getResults.get(0)
      val latLng = result.getGeometry.getLocation
      basicOutputCollector.emit(new Values(time, latLng))
    }
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("time", "geocode"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext): Unit = {
    geocoder = new Geocoder
  }
}
