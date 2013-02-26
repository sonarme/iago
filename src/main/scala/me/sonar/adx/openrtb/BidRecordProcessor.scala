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
import java.io.StringWriter
import com.codahale.jerkson.Json._
import scala.Some
import com.twitter.util.Throw
import com.twitter.util.Return
import scala.util.Random
import org.openrtb._
import com.twitter.util.Throw
import scala.Some
import com.twitter.parrot.util.Uri
import com.twitter.util.Return
import com.twitter.util.Throw
import scala.Some
import com.twitter.parrot.util.Uri
import com.twitter.util.Return

class BidRecordProcessor(service: ParrotService[ParrotRequest, HttpResponse]) extends RecordProcessor {
    val log = Logger.get(getClass)
    val rnd = new Random(System.currentTimeMillis())

    def processLines(job: ParrotJob, lines: Seq[String]) {
        lines flatMap {
            line =>
                val target = job.victims.get(rnd.nextInt(job.victims.size))
                BidParser(line) match {
                    case Return(bidRequest) =>
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