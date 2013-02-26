package me.sonar.adx.openrtb.util

import com.twitter.util.{Throw, Return, Try}
import java.net.URLDecoder
import org.openrtb._
import com.twitter.util.Throw
import com.twitter.util.Return
import collection.JavaConversions._

object BidParser {
    def apply(str: String): Try[BidRequest] = try {
        val bid = str.split(",")
        //(actionType, res, timestamp, bidPrice, handset, jtreqid, pub, site, category, trafficpartner, country, os, zip, city, ciType, clientIp, latlng)
        val actionType = bid(0)
        val res = bid(1)
        val timestamp = bid(2)
        val bidPrice = bid(3)
        val handset = bid(4)
        val jtreqid = bid(5)
        val pub = bid(6)
        val site = bid(7)
        val category = bid(8)
        val trafficpartner = bid(9)
        val country = bid(10)
        val os = bid(11)
        val zip = bid(12)
        val city = bid(13)
        val ciType = bid(14)
        val clientIp = bid(15)
        val Array(lat, lng) = if (bid.length > 16) URLDecoder.decode(bid(16), "UTF-8").split(",").map(_.toDouble) else Array(0.0, 0.0)

        val geoData = new Geo()
        geoData.setLat(lat.toFloat)
        geoData.setLon(lng.toFloat)
        geoData.setCountry(country)
        geoData.setCity(city)
        geoData.setZip(zip)

        val publisher = new Publisher()
        publisher.setId(pub)
        publisher.setCatList(List[String](category))

        val app = new App()
        app.setDomain("sonar.me")
        app.setPublisher(publisher)
        app.setName("sonar")

        val device = new Device()
        device.setIp(clientIp)
        device.setGeo(geoData)
        device.setOs(os)
        device.setMake(handset)

        val bidRequest = new BidRequest("1")
        bidRequest.setImpList(List[Impression]())
        bidRequest.setApp(app)
        bidRequest.setDevice(device)

        Return(bidRequest)
    } catch {
        case t =>
            Throw(t)
    }
}