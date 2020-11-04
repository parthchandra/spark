/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ess

import java.io._
import java.nio.file.{Files, StandardCopyOption}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSCredentialsProviderChain, AWSStaticCredentialsProvider, BasicSessionCredentials, InstanceProfileCredentialsProvider}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, GetObjectRequest}
import io.minio.{MinioClient, PutObjectOptions}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

/**
 * Utility functions for S3-compatible external shuffle storage.
 * This should be used only when dynamic allocation is enabled.
 *
 * These are S3 abstraction functions to support the followings:
 * - AWS S3
 * - MINIO
 * - AWS EFS
 * - HDFS
 */
object StorageShim {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private var s3Client: Option[AmazonS3] = None
  private var minioClient: Option[MinioClient] = None
  private var fs: Option[FileSystem] = None

  private def getOrCreateS3Client(access_key: String, secret_key: String, session_token: String) = {
    s3Client.getOrElse {
      s3Client = Some(AmazonS3ClientBuilder
        .standard()
        .withCredentials(
          new AWSCredentialsProviderChain(
            new AWSStaticCredentialsProvider(
              new BasicSessionCredentials(access_key, secret_key, session_token)),
            new InstanceProfileCredentialsProvider()
          ))
        .build())
      logger.debug("New S3 client is created")
      s3Client.get
    }
  }

  private def getOrCreateMinioClient(access_key: String, secret_key: String, endpoint: String) = {
    minioClient.getOrElse {
      minioClient = Some(new MinioClient(endpoint, access_key, secret_key))
      logger.debug("New Minio client is created")
      minioClient.get
    }
  }

  /** Set FileSystem for hdfs backend */
  def setFileSystem(fs: FileSystem): Unit = this.fs = Some(fs)

  /** Get FileSystem for hdfs backend */
  def getFileSystem(): Option[FileSystem] = fs

  /** Remove all objects starting with the given prefix */
  def cleanUp(
      backend: String,
      access_key: String,
      secret_key: String,
      session_token: String,
      endpoint: String,
      bucket: String,
      prefix: String): Unit = {
    backend match {
      case "s3" =>
        logger.debug(s"Clean up s3://$bucket/$prefix*")
        val s3 = getOrCreateS3Client(access_key, secret_key, session_token)
        // Handle pagination
        var isTruncated = true
        while (isTruncated) {
          val res = s3.listObjectsV2(bucket, prefix)
          if (res.getKeyCount > 0) {
            s3.deleteObjects(new DeleteObjectsRequest(bucket)
              .withKeys(res.getObjectSummaries.asScala.map(_.getKey): _*))
          }
          isTruncated = res.isTruncated
        }

      case "minio" =>
        logger.debug(s"Clean up $endpoint/$bucket/$prefix*")
        val s3 = getOrCreateMinioClient(access_key, secret_key, endpoint)
        val res = s3.listObjects(bucket, prefix, true, true)
        // We need to invoke `get` because this is lazy iterator.
        s3.removeObjects(bucket, res.asScala.map(x => x.get.objectName()).asJava)
          .asScala.foreach(x => x.get)

      case "efs" =>
        logger.debug(s"Clean up $bucket/$prefix")
        val path = new File(s"$bucket/$prefix")
        if (path.exists()) {
          FileUtils.deleteDirectory(new File(s"$bucket/$prefix"))
        } else {
          val list = new File(path.getParent).listFiles()
          if (list != null) {
            list.filter(_.getAbsolutePath.startsWith(s"$bucket/$prefix")).foreach(_.delete)
          }
        }

      case "hdfs" =>
        logger.debug(s"Clean up HDFS $endpoint/$bucket/$prefix")
        val path = new Path(s"$endpoint/$bucket/$prefix")
        val hdfs = fs.get
        if (hdfs.exists(path)) {
          hdfs.delete(path, true)
        } else {
          val prefix = path.toString
          hdfs
            .listStatus(path.getParent)
            .filter(_.getPath.toString.startsWith(prefix))
            .foreach { s => hdfs.delete(s.getPath, true) }
        }
    }
  }

  /** AmazonS3.putObject */
  def putObject(
      backend: String,
      access_key: String,
      secret_key: String,
      session_token: String,
      endpoint: String,
      bucket: String,
      key: String,
      file: File): Unit = {
    // We don't need to waste the resource when the files are already clean up.
    if (!file.exists()) {
      logger.debug(s"Ignore missing file: ${file.getAbsolutePath}")
      return
    }

    backend match {
      case "s3" =>
        logger.debug(s"Upload to s3://$bucket/$key")
        val s3 = getOrCreateS3Client(access_key, secret_key, session_token)
        s3.putObject(bucket, key, file)

      case "minio" =>
        logger.debug(s"Upload to $endpoint/$bucket/$key")
        val s3 = getOrCreateMinioClient(access_key, secret_key, endpoint)
        // Although PutObjectOptions.MAX_PART_SIZE is 5GiB, MinIO has a bug when partSize is
        // greater than 1GiB, e.g. Uploading 100GiB data is silently completed after 20GiB.
        // It's a data loss.
        val option = new PutObjectOptions(
          file.length, if (file.length == 0) -1 else 1024L * 1024 * 1024)
        logger.debug(s"Part Size: ${option.partSize()}, Part Count: ${option.partCount()}")
        s3.putObject(bucket, key, file.getAbsolutePath, option)

      case "efs" =>
        // We don't move the file to be safe and consistent with other module.
        logger.debug(s"Upload to $bucket/$key")
        val path = new File(s"$bucket/$key").toPath
        path.getParent.toFile.mkdirs()
        // To be consistent with S3, StandardCopyOption.REPLACE_EXISTING is used.
        Files.copy(file.toPath, path, StandardCopyOption.REPLACE_EXISTING)

      case "hdfs" =>
        logger.error(s"Upload to HDFS $endpoint/$bucket/$key")
        val hdfs = fs.get

        val path = new Path(s"$endpoint/$bucket/$key")
        // To be consistent with S3, replace the existing file
        hdfs.delete(path, true)
        val os = hdfs.create(path, true)
        Files.copy(file.toPath, os)
        os.close()
    }
  }

  /** AmazonS3.deleteObject */
  def deleteObject(
      backend: String,
      access_key: String,
      secret_key: String,
      session_token: String,
      endpoint: String,
      bucket: String,
      key: String): Unit = {
    backend match {
      case "s3" =>
        logger.debug(s"Delete s3://$bucket/$key")
        val s3 = getOrCreateS3Client(access_key, secret_key, session_token)
        s3.deleteObject(bucket, key)

      case "minio" =>
        logger.debug(s"Delete $endpoint/$bucket/$key")
        val s3 = getOrCreateMinioClient(access_key, secret_key, endpoint)
        s3.removeObject(bucket, key)

      case "efs" =>
        logger.debug(s"Delete $bucket/$key")
        new File(s"$bucket/$key").delete()

      case "hdfs" =>
        logger.debug(s"Delete HDFS $endpoint/$bucket/$key")
        fs.get.delete(new Path(s"$endpoint/$bucket/$key"), true)
    }
  }

  /** AmazonS3.doesObjectExist */
  def doesObjectExist(
      backend: String,
      access_key: String,
      secret_key: String,
      session_token: String,
      endpoint: String,
      bucket: String,
      key: String): Boolean = {
    backend match {
      case "s3" =>
        val s3 = getOrCreateS3Client(access_key, secret_key, session_token)
        logger.debug(s"Check s3://$bucket/$key")
        val result = s3.doesObjectExist(bucket, key)
        if (result) {
          logger.debug(s"Exist s3://$bucket/$key")
        }
        result

      case "minio" =>
        logger.debug(s"Check $endpoint/$bucket/$key")
        try {
          val s3 = getOrCreateMinioClient(access_key, secret_key, endpoint)
          s3.statObject(bucket, key)
          logger.debug(s"Exist $endpoint/$bucket/$key")
          true
        } catch {
          case NonFatal(_) => false
        }

      case "efs" =>
        logger.debug(s"Check $bucket/$key")
        new File(s"$bucket/$key").exists()

      case "hdfs" =>
        logger.debug(s"Check HDFS $endpoint/$bucket/$key")
        fs.get.exists(new Path(s"$endpoint/$bucket/$key"))
    }
  }

  /** AmazonS3.getObject */
  def getObject(
      backend: String,
      access_key: String,
      secret_key: String,
      session_token: String,
      endpoint: String,
      bucket: String,
      key: String,
      start: Long,
      end: Long)
    : InputStream = {
    backend match {
      case "s3" =>
        val s3 = getOrCreateS3Client(access_key, secret_key, session_token)
        logger.debug(s"Read [$start:$end] from s3://$bucket/$key")
        s3.getObject(new GetObjectRequest(bucket, key).withRange(start, end)).getObjectContent

      case "minio" =>
        logger.debug(s"Read [$start:$end] from $endpoint/$bucket/$key")
        val s3 = getOrCreateMinioClient(access_key, secret_key, endpoint)
        s3.getObject(bucket, key, start, end)

      case "efs" =>
        logger.debug(s"Read [$start:$end] from $bucket/$key")
        val fis = new FileInputStream(s"$bucket/$key")
        fis.skip(start)
        fis

      case "hdfs" =>
        logger.debug(s"Read [$start:$end] from HDFS $endpoint/$bucket/$key")
        val fis = fs.get.open(new Path(s"$endpoint/$bucket/$key"))
        fis.skip(start)
        fis
    }
  }
}
