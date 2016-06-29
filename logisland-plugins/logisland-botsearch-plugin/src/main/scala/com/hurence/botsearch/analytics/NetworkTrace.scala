package com.hurence.botsearch.analytics

case class NetworkTrace(ipSource: String,
						ipTarget: String,
						avgUploadedBytes: Float,
						avgDownloadedBytes: Float,
						avgTimeBetweenTwoFLows: Float,
						mostSignificantFrequency: Float,
						flowsCount: Int,
						tags: String,
						centroid: Int = 0)