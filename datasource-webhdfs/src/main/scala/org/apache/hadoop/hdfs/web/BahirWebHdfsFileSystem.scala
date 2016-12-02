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

package org.apache.hadoop.hdfs.web

import java.io.IOException
import java.net.{HttpURLConnection, URI, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.hdfs.web.resources.{BufferSizeParam, GetOpParam, OffsetParam, Param}
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Op

/**
 * A FileSystem for HDFS over the web, extending [[org.apache.hadoop.hdfs.web.WebHdfsFileSystem]]
 * to allow secure authentication and configurable gateway path segments.
 *
 * TODO: upgrade Hadoop version (Maven dependencies) to override SWebHdfsFileSystem
 */
class BahirWebHdfsFileSystem extends WebHdfsFileSystemCopy {

  // TODO: figure out how/where to authenticate

  // TODO: gateway path should be configurable
  val gatewayPath = "/gateway/default"


  override def initialize(uri: URI, conf: Configuration): Unit = {

    // scalastyle:off println
    println("Hi there " + this.getClass + ".initialize(uri: URI, conf: Configuration)")
    // scalastyle:on println

    super.initialize(uri, conf)
  }

  override def toUrl(op: Op, fspath: Path, parameters: Param[_, _]*): URL = {
    val url = super.toUrl(op, fspath, parameters: _*)

    // scalastyle:off println
    println("Hi there " + this.getClass + ".toUrl(op: Op, fspath: Path, parameters: Param[_, _]*)")
    // scalastyle:on println

    new URL(url.getProtocol, url.getHost, url.getPort,
      url.getFile.replaceFirst(WebHdfsFileSystem.PATH_PREFIX,
        gatewayPath + WebHdfsFileSystem.PATH_PREFIX))
  }

  // hadoop 2.7.3
  @throws[IOException]
  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    statistics.incrementReadOps(1)

    // scalastyle:off println
    println("Hi there " + this.getClass + ".open(f: Path, bufferSize: Int)")
    // scalastyle:on println

    val webHdfsInputStream: WebHdfsInputStream = new WebHdfsInputStream(f, bufferSize)
    webHdfsInputStream.setReadRunner(new BahirReadRunner(f, bufferSize))
    new FSDataInputStream(webHdfsInputStream)
  }

  class BahirReadRunner(p: Path, bs: Int) extends ReadRunner(p: Path, bs: Int) {
    override def getUrl: URL = {
      val url = super.getUrl

      // scalastyle:off println
      println("Hi there " + this.getClass + ".BahirReadRunner.getUrl(f: Path, bs: Int)")
      // scalastyle:on println

      new URL(url.getProtocol, url.getHost, url.getPort,
        url.getFile.replaceFirst(WebHdfsFileSystem.PATH_PREFIX,
          gatewayPath + WebHdfsFileSystem.PATH_PREFIX))
    }
  }
}
