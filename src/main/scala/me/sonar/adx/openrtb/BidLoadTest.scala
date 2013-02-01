package me.sonar.adx.openrtb

import com.twitter.parrot.server.{ParrotRequest, ParrotService}
import org.jboss.netty.handler.codec.http.HttpResponse
import com.twitter.parrot.config.ParrotServerConfig
import com.twitter.parrot.processor.RecordProcessor
import com.twitter.logging.Logger
import com.twitter.parrot.thrift.ParrotJob
import com.twitter.parrot.util.UriParser
import com.twitter.util.{Throw, Return}
import com.twitter.ostrich.stats.Stats

class BidLoadTest(service: ParrotService[ParrotRequest, HttpResponse],
                  config: ParrotServerConfig[ParrotRequest, HttpResponse])
        extends RecordProcessor {
    val log = Logger.get(getClass)

    def processLines(job: ParrotJob, lines: Seq[String]) {
        lines flatMap {
            line =>
                val target = job.victims.get(config.randomizer.nextInt(job.victims.size))
                UriParser(line) match {
                    case Return(uri) =>
                        if (!uri.path.isEmpty && !line.startsWith("#")) {
                            val body = "{\"id\" : \"BidRequest1\", \"at\" : 1, \"tmax\" : 100,  \"imp\" : [ {\"impid\" : \"BidRequest1Impression1\", \"wseat\" : [ \"seat\" ],    \"h\" : 200,    \"w\" : 300,    \"pos\" : 18,    \"instl\" : 18,    \"btype\" : [ \"btype\" ],    \"battr\" : [ \"battr\" ] } ],  \"site\" : {\"sid\": \"sonar.me\", \"name\": \"sonar.me\", \"domain\": \"sonar.me\", \"pid\": \"pid\", \"pub\": \"pub\", \"pdomain\": \"pdomain\", \"cat\": [ ], \"keywords\": \"foo,bar,keywords\",  \"page\": \"page\", \"ref\":\"ref\", \"search\": \"search\"  }, \"app\" : null,  \"device\" : { \"did\": \"foo\", \"dpid\":\"asdf\", \"country\": \"USA\", \"carrier\":\"carrier\", \"ua\": \"ua\", \"make\":\"make\", \"model\":\"iphone\", \"os\":\"ios\", \"osv\":\"5\", \"js\":0, \"loc\": \"40.750580,-73.993580\", \"ip\":\"69.38.227.134\"},  \"user\" : {\"uid\":\"bar\", \"yob\":4, \"gender\":\"male\", \"zip\":\"10003\", \"country\":\"USA\", \"keywords\":\"keyword\"},  \"restrictions\": null }"
                            val request = new ParrotRequest(target, None, Nil, uri, line, method = "POST", body = body)
                            Some(service(request))
                        }
                        else
                            None
                    case Throw(t) =>
                        Stats.incr("bad_lines")
                        Stats.incr("bad_lines/" + t.getClass.getName)
                        None
                }
        }
    }
}