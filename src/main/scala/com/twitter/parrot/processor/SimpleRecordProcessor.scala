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
import com.twitter.parrot.util.UriParser
import com.twitter.util.{Return, Throw}
import org.jboss.netty.handler.codec.http.HttpResponse
import com.google.common.net.InetAddresses
import java.util.Random

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
    lines flatMap { line =>
      val target = job.victims.get(config.randomizer.nextInt(job.victims.size))
      UriParser(line) match {
        case Return(uri) =>
          if (!uri.path.isEmpty && !line.startsWith("#")) {
              val ip = InetAddresses.fromInteger(new Random().nextInt()).getHostAddress
            val body = "{\"id\" : \"BidRequest1\", \"at\" : 1, \"tmax\" : 100,  \"imp\" : [ {\"impid\" : \"BidRequest1Impression1\", \"wseat\" : [ \"seat\" ],    \"h\" : 200,    \"w\" : 300,    \"pos\" : 18,    \"instl\" : 18,    \"btype\" : [ \"btype\" ],    \"battr\" : [ \"battr\" ] } ],  \"site\" : {\"sid\": \"sonar.me\", \"name\": \"sonar.me\", \"domain\": \"sonar.me\", \"pid\": \"pid\", \"pub\": \"pub\", \"pdomain\": \"pdomain\", \"cat\": [ ], \"keywords\": \"foo,bar,keywords\",  \"page\": \"page\", \"ref\":\"ref\", \"search\": \"search\"  }, \"app\" : null,  \"device\" : { \"did\": \"foo\", \"dpid\":\"asdf\", \"country\": \"USA\", \"carrier\":\"carrier\", \"ua\": \"ua\", \"make\":\"make\", \"model\":\"iphone\", \"os\":\"ios\", \"osv\":\"5\", \"js\":0, \"loc\": \"40.750580,-73.993580\", \"ip\":\"" + ip + "\"},  \"user\" : {\"uid\":\"bar\", \"yob\":4, \"gender\":\"male\", \"zip\":\"10003\", \"country\":\"USA\", \"keywords\":\"keyword\"},  \"restrictions\": null }"
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
