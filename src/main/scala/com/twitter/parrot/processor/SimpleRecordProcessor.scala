/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.parrot.processor

import com.twitter.ostrich.stats.Stats
import com.twitter.parrot.config.ParrotServerConfig
import com.twitter.parrot.server.{ParrotRequest, ParrotService}
import com.twitter.parrot.thrift.ParrotJob
import com.twitter.parrot.util.{Uri, UriParser}
import com.twitter.util.{Return, Throw}
import org.jboss.netty.handler.codec.http.HttpResponse
import com.google.common.net.InetAddresses
import java.util.Random
import com.sonar.dossier.dto.GeodataDTO
import com.sonar.expedition.common.adx.search.model._
import com.codahale.jerkson.Json._
import java.io.StringWriter
import me.sonar.adx.openrtb.util.BidParser
import com.twitter.util.Throw
import scala.Some
import com.twitter.util.Return
import com.sonar.dossier.dto.GeodataDTO
import scala.Some
import com.twitter.util.Throw
import com.twitter.util.Return
import com.sonar.dossier.dto.GeodataDTO
import com.sonar.expedition.common.adx.search.model.App
import com.sonar.expedition.common.adx.search.model.Device
import scala.Some
import com.sonar.expedition.common.adx.search.model.Publisher
import com.twitter.util.Throw
import com.sonar.expedition.common.adx.search.model.Impression
import com.sonar.expedition.common.adx.search.model.BidRequest
import com.twitter.util.Return
import com.sonar.dossier.dto.GeodataDTO

/**
 * This processor just takes a line-separated list of URIs and turns them into requests, for instance:
 * /search.json?q=%23playerofseason&since_id=68317051210563584&rpp=30
 * needs to have the target scheme, host, and port included, but otherwise is a complete URL
 * Empty lines and lines starting with '#' will be ignored.
 */
class SimpleRecordProcessor(service: ParrotService[ParrotRequest, HttpResponse],
                            config: ParrotServerConfig[ParrotRequest, HttpResponse])
        extends RecordProcessor {

    def processLines(job: ParrotJob, lines: Seq[String]) {
        lines flatMap {
            line =>
                val target = job.victims.get(config.randomizer.nextInt(job.victims.size))
                BidParser(line) match {
                    case Return(bid) =>
                            val Array(lat, lng) = if (bid.latlng != null) bid.latlng.split(",").map(_.toDouble) else Array(0.0, 0.0)
                                                    val geoData = Geo(lat, lng, country = bid.country, city = bid.city, zip = bid.zip)

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
