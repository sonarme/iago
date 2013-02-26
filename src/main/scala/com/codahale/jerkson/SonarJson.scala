package com.codahale.jerkson

import org.codehaus.jackson.map.{MappingJsonFactory, ObjectMapper}
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.{JsonParser, JsonGenerator}
import org.codehaus.jackson.{JsonGenerator, JsonParser => JacksonParser}

object SonarJson extends SonarJson

trait SonarJson extends Parser with Generator {
    protected val classLoader = Thread.currentThread().getContextClassLoader

    protected val mapper = new ObjectMapper
    mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL)
    mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_DEFAULT)
    mapper.registerModule(new ScalaModule(classLoader))

    protected val factory = new MappingJsonFactory(mapper)
    factory.enable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT)
    factory.enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
    factory.enable(JsonGenerator.Feature.QUOTE_FIELD_NAMES)
    factory.enable(JacksonParser.Feature.ALLOW_COMMENTS)
    factory.enable(JacksonParser.Feature.AUTO_CLOSE_SOURCE)

}