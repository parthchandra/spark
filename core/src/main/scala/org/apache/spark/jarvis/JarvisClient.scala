package org.apache.spark.jarvis

import java.io.IOException
import org.apache.http.entity.mime.content.StringBody
import org.apache.http.{HttpEntity, HttpResponse, NameValuePair}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder, StandardHttpRequestRetryHandler}
import org.apache.http.util.EntityUtils
import org.apache.spark.Logging
import org.apache.http.auth.AuthScope
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.auth.AuthenticationException
import scala.util.control.Breaks._

/**
 * JarvisClient creates and submits requests
 * to jarvis
 *
 * TODO :
 * [ ] Add status listener support
 * [ ] Fix builders
 * [ ] Fix retries when Jarvis is unavailable
 *
 * Created by vishwa on 2/2/16.
 */
private [spark] class JarvisClient extends Logging {

  var uri:String = ""
  var maxRetries = 3
  var socketTimeoutSecs = 15
  var connectionTimeoutSecs = 15
  var connectionRequestTimeoutSecs = 15
  var httpClient: CloseableHttpClient = null
  

  val serviceId = "serviceId"
  def fibonacci(n : Int) : Int = {
    def fibotail( n: Int, a:Int, b:Int): Int = {
      if (n > 0)
        fibotail( n-1, b, a+b )
      else
        a
    }
    return fibotail( n, 0, 1)
  }

  def submit(jobs: List[JarvisJobInfo], username: String, password: String): Unit = {
    val jobGroup = new JarvisJobGroup(jobs)
    val serviceConfig = new JarvisServiceConfig
    serviceConfig.addJobGroup(jobGroup)
    submitJob(serviceConfig, username, password)
  }

  def submitJob(config: JarvisServiceConfig, username: String, password: String): Unit = {
    try {
      logInfo("Submitting Job with username=["+username+"], password=["+password+"]")
      postRequest(config.toJsonString, username, password)
    } catch {
      case e:Exception =>
        logError("Exception submitting job to jarvis " + e, e)
    }
  }

  def executeWithRetries(request: HttpPost, localContext: HttpClientContext): HttpResponse = {
    var response:HttpResponse = null;
    var i = 0;
    
    while (i < maxRetries && response == null) {
      i+=1
      try {
        logInfo("Sending request to jarvis =" + request + ", request =" + request.getEntity)
        response = httpClient.execute(request, localContext)

        if (response != null && response.getEntity != null)
          logInfo("Response from jarvis : "+EntityUtils.toString(response.getEntity));

      } catch {
        case e:AuthenticationException => {
          logError("Authentication Failed while connecting to Jarvis environment. Will not retry.", e)
          break
        }
        case e:IOException => {
          var sleepTimeInSeconds = fibonacci(i)
          logError("Exception while sending request to jarvis. retryCount = [" + i + "/" + maxRetries + "]. Sleeping [" + sleepTimeInSeconds + "] seconds before retry. e = " + e)
          Thread.sleep(sleepTimeInSeconds*1000)
        }
      }
    }

    response
  }

  def postRequest(jobJson: String, username: String, password: String): Unit = {
    val input:HttpEntity = MultipartEntityBuilder.create()
      .addPart("serviceConfig", new StringBody(jobJson, ContentType.APPLICATION_JSON))
      .addTextBody(serviceId, "sparkonjarvis-" + System.currentTimeMillis())
      .build()

    var request = new HttpPost(uri);
    request.setEntity(input);

    var credentialsProvider: CredentialsProvider = new BasicCredentialsProvider();
    var localContext: HttpClientContext = HttpClientContext.create();

    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username + ":" + password));
    localContext.setCredentialsProvider(credentialsProvider);

    logInfo("Creating jarvis request with data " + jobJson)
    executeWithRetries(request, localContext);
  }

  override def toString:String = {
    "JarvisClient : uri : " + uri +
      ", maxRetries = " + maxRetries +
      ", socketTimeoutSecs = " + socketTimeoutSecs +
      ", connectionTimeoutSecs = " + connectionTimeoutSecs +
      ", connectionRequestTimeoutSecs = " + connectionRequestTimeoutSecs
  }
}

object JarvisClient extends Logging {

  def builder:JarvisClientBuilder = new JarvisClientBuilder

  class JarvisClientBuilder {
    var inner: JarvisClient = new JarvisClient

    def withUri(uri: String): JarvisClientBuilder = {
      inner.uri = uri
      this
    }

    def withSocketTimeoutSeconds(socketTimeoutSecs: Int): JarvisClientBuilder = {
      if (socketTimeoutSecs > 0) inner.socketTimeoutSecs = socketTimeoutSecs
      this
    }

    def withConnectionTimeoutSeconds(connTimeoutSecs: Int): JarvisClientBuilder = {
      if (connTimeoutSecs > 0) inner.connectionTimeoutSecs = connTimeoutSecs
      this
    }

    def withConnectionRequestTimeoutSeconds(connectionReqTimeoutSecs: Int): JarvisClientBuilder = {
      if (connectionReqTimeoutSecs > 0) inner.connectionRequestTimeoutSecs = connectionReqTimeoutSecs
      this
    }

    def build: JarvisClient = {

      var requestConfig = RequestConfig.custom
        .setSocketTimeout(inner.socketTimeoutSecs * 1000)
        .setConnectTimeout(inner.connectionTimeoutSecs * 1000)
        .setConnectionRequestTimeout(inner.connectionRequestTimeoutSecs * 1000)
        .setStaleConnectionCheckEnabled(true)
        .build()

      var httpClientBuilder = HttpClientBuilder.create
      httpClientBuilder.setDefaultRequestConfig(requestConfig)
      httpClientBuilder.setRetryHandler(new StandardHttpRequestRetryHandler())

      inner.httpClient = httpClientBuilder.build()

      logInfo("Creating jarvis client : " + inner)
      inner
    }
  }
}