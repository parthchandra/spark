package org.apache.spark.jarvis

import java.util.UUID

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonIgnore, JsonInclude}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

@JsonInclude(Include.NON_NULL)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
private[spark] class JarvisJobInfo {

  @JsonIgnore
  var uuid: UUID = UUID.randomUUID()
  var name: String = ""
  var instances = 1
  var cpus = 0
  var memory = 50
  var disk = 0
  var ports = 0
  var artifacts: List[String] = List()
  var uris: List[String] = List()

  @JsonIgnore
  var environment = ""

  var command = ""

  @JsonIgnore
  var status: String = Initialized
  @JsonIgnore
  val Initialized: String = "Initialized"
  @JsonIgnore
  val Waiting: String = "Waiting"
  @JsonIgnore
  val Running: String = "Running"
  @JsonIgnore
  val Completed: String = "Completed"


  override def toString: String = {
    "JobInfo : name = " + name +
      ", instances = " + instances +
      ", memory = " + memory +
      ", cpus = " + cpus +
      ", disk = " + disk +
      ", ports = " + ports +
      ", environment = " + environment +
      ", command = " + command
  }

  def toJson = JarvisJobInfo.mapper.writeValueAsString(this)

}

private[spark] object JarvisJobInfo {
  private val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)

  def builder: JobBuilder = {
    new JobBuilder
  }

  class JobBuilder {
    var inner: JarvisJobInfo = new JarvisJobInfo

    def withName(name: String): JobBuilder = {
      inner.name = name
      this
    }

    def withInstances(instances: Integer): JobBuilder = {
      inner.instances = instances
      this
    }

    def withMemory(memory: Integer): JobBuilder = {
      inner.memory = memory
      this
    }

    def withCpus(cpus: Integer): JobBuilder = {
      inner.cpus = cpus
      this
    }

    def withDisk(disks: Integer): JobBuilder = {
      inner.disk = disks
      this
    }

    def withPorts(ports: Integer): JobBuilder = {
      inner.ports = ports
      this
    }

    def withArtifacts(artifacts: List[String]): JobBuilder = {
      inner.artifacts = artifacts
      this
    }

    def withEnvironment(environment: String): JobBuilder = {
      inner.environment = environment
      this
    }

    def withCommand(command:String): JobBuilder = {
      inner.command = command
      this
    }

    def withUris(uris: List[String]): JobBuilder = {
      inner.uris = uris
      this
    }

    def build: JarvisJobInfo = {
      inner
    }
  }
}
