package org.apache.spark

import java.util.UUID

import org.apache.mesos.Protos.{SlaveID, FrameworkID, OfferID, Offer}
import org.apache.spark.jarvis.{JarvisClient, JarvisJobInfo}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.mesos.CoarseMesosSchedulerBackend

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 *
 * JarvisSchedulerBackend is responsible for scheduling coarse-grained
 * tasks on Jarvis.
 *
 * The tasks are launched on coarse-grained spark workers which are
 * started by Jarvis.
 *
 * TODO
 * [ ] Fix resubmit logic
 * [ ] Move hardcoded values to configurations
 * [ ] Add shutdown through jarvis on stop()
 *
 * Created by vishwa on 12/11/15.
 */
class JarvisSchedulerBackend(scheduler: TaskSchedulerImpl, sc: SparkContext, jarvisUrl: String) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) with Logging{

  private[this] var lastIsReadyLog = 0L

  val maxCores = conf.getInt("spark.cores.max", 4)
  val maxCoresPerJob = conf.getInt("spark.jarvis.cores.per.job.max", 2)
  val sparkMesosScheduler = new CoarseMesosSchedulerBackend(scheduler, sc, "", sc.env.securityManager)
  var username = conf.get("spark.jarvis.user.name", null)
  var password = conf.get("spark.jarvis.password", null)

  val totalCoresRequested = 0;

  val jarvisClient: JarvisClient = {
    JarvisClient.builder
      .withUri(jarvisUrl)
      .build
  }

  def createJob(numCores: Int): JarvisJobInfo = {
    val jobId = UUID.randomUUID()
    logInfo(s"Creating job with id: $jobId")

    val fakeOffer = Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue("Jarvis"))
      .setFrameworkId(FrameworkID.newBuilder().setValue("SparkOnJarvis"))
      .setHostname("localhost")
      .setSlaveId(SlaveID.newBuilder().setValue(jobId.toString))
      .build()

    val taskId = sparkMesosScheduler.newMesosTaskId()
    val commandInfo = sparkMesosScheduler.createCommand(fakeOffer, numCores, taskId)
    val environmentInfo = commandInfo.getEnvironment

    val uris = commandInfo.getUrisList.asScala.map{ uri => uri.getValue }

    val environment = environmentInfo.getVariablesList.asScala
        .map{ v => (v.getName, v.getValue) }.toMap + ("SPARK_LOCAL_DIRS" -> "spark-temp")

    val commandString = commandInfo.getValue

    val commands = environment ++ Seq(commandString)

    var job = JarvisJobInfo.builder
      .withCommand(commands.mkString("; "))
      .withCpus(numCores)
      .withMemory(sparkMesosScheduler.calculateTotalMemory(sc))
      .withUris(uris.toList)
      .withName("TestJarvis")
      .withEnvironment("staging")
      .build

    logInfo ("Created job = " + job)
    job
  }

  def createJobs(): List[JarvisJobInfo] = {

    var jobs:ListBuffer[JarvisJobInfo] = ListBuffer.empty[JarvisJobInfo]
    var availableCores = maxCores;

    while (availableCores > 0) {

      if (availableCores < maxCoresPerJob)
        jobs += createJob(availableCores)
      else
        jobs += createJob(maxCoresPerJob)

      availableCores -= maxCoresPerJob
    }

    jobs.toList
  }

  def submitCoarseGrainedJobs(): Unit = {
    val jobs = createJobs()
    jarvisClient.submit(jobs, username, password);
  }

  override def start():Unit = {
    super.start()
    submitCoarseGrainedJobs()
  }


  override def stop(): Unit = {
    super.stop()
  }

  override def isReady(): Boolean = {
    val ready = super.isReady();
    val currentTime = System.currentTimeMillis()

    if (!ready && currentTime - lastIsReadyLog > 5000) {
      logWarning("Backend is not yet ready");
    }

    ready
  }
}
