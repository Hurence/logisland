/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.botsearch.analytics

import java.text.SimpleDateFormat

import com.typesafe.scalalogging.slf4j.LazyLogging


case class NetworkFlow(timestamp: Long, method: String, ipSource: String, ipTarget: String, urlScheme: String, urlHost: String, urlPort: String, urlPath: String, requestSize: Int, responseSize: Int, isOutsideOfficeHours: Boolean, isHostBlacklisted: Boolean, tags: String)


object NetworkFlow extends LazyLogging {

  /**
   * take a line of csv and convert it to a NetworkFlow
   *
   * @param line
   * @return
   */
  def parse(line: String): NetworkFlow = {
	try {
	  val records = line.split("\t")
	  val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
	  val timestamp = try{
		sdf.parse(records(0)).getTime
	  }catch{
		case t: Throwable => 0
	  }

	  val tags = if (records.length == 13)
		records(12).replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", "")
	  else ""

	  new NetworkFlow(timestamp,
		records(1),
		records(2),
		records(3),
		records(4),
		records(5),
		records(6),
		records(7),
		records(8).toInt,
		records(9).toInt,
		records(10).toBoolean,
		records(11).toBoolean,
		tags)
	} catch {
	  case t: Exception => {
		logger.error(s"exception parsing row : ${t.getMessage}")
		new NetworkFlow(0, "unknown", "unknown", "unknown", "unknown", "unknown", "unknown", "unknown", 0, 0, false, false, "")
	  }
	}
  }


  def dump(networkFlow: NetworkFlow): String = {
	""
	//s"$timestamp\t$method, $ipSource, $ipTarget, $urlScheme, $urlHost, $urlPort, $urlPath, $requestSize, $responseSize, $isOutsideOfficeHours, $isHostBlacklisted, $tags"
  }
}


