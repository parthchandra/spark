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

import java.io.File
import java.nio.file.Files

import org.scalatest.FunSuite // scalastyle:ignore funsuite

import org.apache.spark.ess.StorageShim._

class StorageShimSuite extends FunSuite { // scalastyle:ignore funsuite

  test("Shim API test with efs") {
    val backend = "efs"
    val accessKey = ""
    val secretKey = ""
    val endpoint = ""
    val bucket = Files.createTempDirectory("tmp").toFile.getAbsolutePath
    val file = File.createTempFile("tmp", ".tmp")

    val index1 = "appid/shuffle_0_0_0.index"
    val data1 = "appid/shuffle_0_0_0.data"
    val index2 = "appid/shuffle_0_1_0.index"
    val index3 = "appid/shuffle_1_2_0.index"
    val data3 = "appid/shuffle_1_2_0.data"

    putObject(backend, accessKey, secretKey, endpoint, bucket, index1, file)
    putObject(backend, accessKey, secretKey, endpoint, bucket, data1, file)
    assert(doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, index1))
    assert(doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, data1))

    deleteObject(backend, accessKey, secretKey, endpoint, bucket, data1)
    assert(doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, index1))
    assert(!doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, data1))

    // Clean up an app
    cleanUp(backend, accessKey, secretKey, endpoint, bucket, "appid")
    assert(!doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, data1))

    // Clean up a shuffle
    putObject(backend, accessKey, secretKey, endpoint, bucket, index1, file)
    putObject(backend, accessKey, secretKey, endpoint, bucket, index2, file)
    putObject(backend, accessKey, secretKey, endpoint, bucket, index3, file)
    cleanUp(backend, accessKey, secretKey, endpoint, bucket, "appid/shuffle_0_")
    assert(!doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, index1))
    assert(!doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, index2))
    assert(doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, index3))

    // Clean up a file
    cleanUp(backend, accessKey, secretKey, endpoint, bucket, data3)
    assert(!doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, data3))
  }
}

