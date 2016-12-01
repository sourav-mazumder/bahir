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
import java.net.{HttpURLConnection, URL}

import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.hdfs.ByteRangeInputStream
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.OffsetUrlInputStream
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Op
import org.apache.hadoop.hdfs.web.resources.{BufferSizeParam, GetOpParam, OffsetParam, Param}

/**
 * A FileSystem for HDFS over the web, extending [[org.apache.hadoop.hdfs.web.WebHdfsFileSystem]]
 * to allow secure authentication and configurable gateway path segments.
 *
 * TODO: upgrade Hadoop version (Maven dependencies) to override SWebHdfsFileSystem
 */
class BahirWebHdfsFileSystem extends WebHdfsFileSystem {

  // TODO: figure out how/where to authenticate

  // TODO: gateway path should be configurable
  val gatewayPath = "/gateway/default"


  override def toUrl(op: Op, fspath: Path, parameters: Param[_, _]*): URL = {
    val url = super.toUrl(op, fspath, parameters: _*)

    new URL(url.getProtocol, url.getHost, url.getPort,
      url.getFile.replaceFirst(WebHdfsFileSystem.PATH_PREFIX,
        gatewayPath + WebHdfsFileSystem.PATH_PREFIX))
  }

//  @throws[IOException]
//  override def open(f: Path, buffersize: Int): FSDataInputStream = {
//    statistics.incrementReadOps(1)
//    val op: Op = GetOpParam.Op.OPEN
//    val url: URL = toUrl(op, f, new BufferSizeParam(buffersize))
//    new FSDataInputStream(new WebHdfsFileSystem.OffsetUrlInputStream(
//      new WebHdfsFileSystem.OffsetUrlOpener(url), new WebHdfsFileSystem#OffsetUrlOpener(null)))
//  }

  // hadoop 2.2
  @throws[IOException]
  override def open(f: Path, buffersize: Int): FSDataInputStream = {
    statistics.incrementReadOps(1)
    val op: Op = GetOpParam.Op.OPEN
    val url: URL = toUrl(op, f, new BufferSizeParam(buffersize))
    val offsetUrlInputStream: OffsetUrlInputStream = new WebHdfsFileSystem.OffsetUrlInputStream(
      new OffsetUrlOpener(url), new OffsetUrlOpener(null))

    val inputStream = new FSDataInputStream(offsetUrlInputStream)

//    val inputStream = super.open(f, buffersize)
//    val wrappedInputStream = inputStream.getWrappedStream

    inputStream
  }

  // hadoop 2.7.3
//  @throws[IOException]
//  def open(f: Path, bufferSize: Int): FSDataInputStream = {
//    statistics.incrementReadOps(1)
//    new FSDataInputStream(new WebHdfsInputStream(f, bufferSize))
//  }


}
