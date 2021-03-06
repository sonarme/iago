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

import collection.JavaConversions._
import com.twitter.parrot.server.{ParrotRequest, ParrotService, ParrotThriftServiceWrapper}
import com.twitter.parrot.thrift.ParrotJob

trait RecordProcessor {
  def start(job: ParrotJob): Unit = { }
  def processLines(job: ParrotJob, lines: Seq[String])
  def changeJob(job: ParrotJob): Unit = { }
  def shutdown(): Unit = { }
}

abstract class ThriftRecordProcessor(parrotService: ParrotService[ParrotRequest, Array[Byte]])
extends RecordProcessor {
  val service = new ParrotThriftServiceWrapper(parrotService)
}

/**
 * This is here to support Java users of Parrot. Scala users should simply mixin the RecordProcessor trait
 */
abstract class LoadTest extends RecordProcessor {
  def processLines(job: ParrotJob, lines: java.util.List[String])

  def processLines(job: ParrotJob, lines: Seq[String]) {
    val linesAsList: java.util.List[String] = lines
    processLines(job, linesAsList)
  }
}

abstract class ThriftLoadTest(pService: ParrotService[ParrotRequest, Array[Byte]]) extends ThriftRecordProcessor(pService) {
  def processLines(job: ParrotJob, lines: java.util.List[String])

  def processLines(job: ParrotJob, lines: Seq[String]) {
    val linesAsList: java.util.List[String] = lines
    processLines(job, linesAsList)
  }
}
