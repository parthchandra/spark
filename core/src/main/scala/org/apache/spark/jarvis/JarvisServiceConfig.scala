package org.apache.spark.jarvis

import com.fasterxml.jackson.annotation.{JsonInclude, JsonAutoDetect}
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{SerializationFeature, DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Created by vishwa on 2/8/16.
 */
@JsonInclude(Include.NON_NULL)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
class JarvisServiceConfig {

  var daemonJobGroups = Seq[JarvisJobGroup]()

  def addJobGroup(jobGroup: JarvisJobGroup) =
    daemonJobGroups = daemonJobGroups :+ (jobGroup)

  def toJsonString = JarvisServiceConfig.mapper.writeValueAsString(this)

}

private[spark] object JarvisServiceConfig {

  private val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)
}
