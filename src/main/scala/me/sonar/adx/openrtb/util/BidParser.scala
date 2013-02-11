package me.sonar.adx.openrtb.util

import com.twitter.util.{Throw, Return, Try}
import java.net.URLDecoder

case class Bid(actionType: String = null,
               errid: String = null,
               timestamp: String = null,
               bidPrice: String = null,
               handset: String = null,
               jtreqid: String = null,
               pub: String = null,
               site: String = null,
               category: String = null,
               trafficpartner: String = null,
               country: String = null,
               os: String = null,
               zip: String = null,
               city: String = null,
               ciType: String = null,
               clientIp: String = null,
               lat: Double = 0.0,
               lng: Double = 0.0)

object BidParser {
    def apply(str: String): Try[Bid] = try {
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
        Return(Bid(actionType, res, timestamp, bidPrice, handset, jtreqid, pub, site, category, trafficpartner, country, os, zip, city, ciType, clientIp, lat, lng))
    } catch {
        case t =>
            Throw(t)
    }
}