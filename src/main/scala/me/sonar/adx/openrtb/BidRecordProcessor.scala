package me.sonar.adx.openrtb

import com.twitter.parrot.server.{ParrotRequest, ParrotService}
import org.jboss.netty.handler.codec.http.HttpResponse
import com.twitter.parrot.config.ParrotServerConfig
import com.twitter.parrot.processor.RecordProcessor
import com.twitter.logging.Logger
import com.twitter.parrot.thrift.ParrotJob
import com.twitter.parrot.util.{Uri, UriParser}
import com.twitter.util.{Throw, Return}
import com.twitter.ostrich.stats.Stats
import util.BidParser
import com.sonar.expedition.common.adx.search.model._
import scala.Some
import com.twitter.util.Throw
import com.twitter.util.Return
import com.sonar.expedition.common.adx.search.model.App
import com.sonar.expedition.common.adx.search.model.Geo
import scala.Some
import com.sonar.expedition.common.adx.search.model.Publisher
import com.twitter.util.Throw
import com.sonar.expedition.common.adx.search.model.Impression
import com.sonar.expedition.common.adx.search.model.BidRequest
import com.twitter.util.Return
import java.io.StringWriter
import com.codahale.jerkson.Json._
import com.sonar.expedition.common.adx.search.model.App
import com.sonar.expedition.common.adx.search.model.Geo
import com.sonar.expedition.common.adx.search.model.Device
import scala.Some
import com.sonar.expedition.common.adx.search.model.Publisher
import com.twitter.util.Throw
import com.sonar.expedition.common.adx.search.model.Impression
import com.sonar.expedition.common.adx.search.model.BidRequest
import com.twitter.util.Return
import scala.util.Random

class BidRecordProcessor(service: ParrotService[ParrotRequest, HttpResponse]) extends RecordProcessor {
    val log = Logger.get(getClass)
    val rnd = new Random(System.currentTimeMillis())

    def processLines(job: ParrotJob, lines: Seq[String]) {
        lines flatMap {
            line =>
                val target = job.victims.get(rnd.nextInt(job.victims.size))
                BidParser(line) match {
                    case Return(bid) =>
                        val geoData = Geo(bid.lat, bid.lng, country = bid.country, city = bid.city, zip = bid.zip)

                        val bidRequest = BidRequest("1", List[Impression](), app = App(bid.site, name = "sonar", domain = "sonar.me", publisher = Publisher(id = bid.pub, cat = List[String](bid.category))), device = Device(ip = bid.clientIp, geo = geoData, os = bid.os, make = bid.handset))

                        val writer = new StringWriter
                        generate[BidRequest](bidRequest, writer)
                        val body = writer.toString
                        val request = new ParrotRequest(target, None, Nil, Uri("/bid", Nil), line, method = "POST", body = body)
                        Some(service(request))
                    case Throw(t) =>
                        Stats.incr("bad_lines")
                        Stats.incr("bad_lines/" + t.getClass.getName)
                        None
                }
        }
    }
}