/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.botsearch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * An HttpFlow is an entity that grabbers information from a log event.
 *
 * Actually the inplementation gets data from SERAI log files and insert
 * geolocation data + week number.
 *
 * @author tom
 */
public class HttpFlow implements Serializable{

	// time
	private Date date = new Date(0);
//	private int weekNumber;
	private Boolean isOutsideOfficeHours;
	private Boolean isHostBlacklisted;

	// http query 
	private String ipSource = "";
	private String ipTarget = "";
	private String httpMethod;
	private String httpResponse;
	private long requestSize;
	private long responseSize;
	private String userAgent = "";
//	private String os = "";
//	private String browser = "";
	private Boolean isTunnel;
	private Boolean isIp;
	private Boolean isBadAgent;
	private Boolean isHttp10;
	private Boolean isBadCert;
//	private String awkString;
//	private String http10;

	// url
	private String urlHost = "";
	private int urlPort;
	private String urlPath = "";
	private String urlScheme = "";
	private List<String> urlQueryKeys = new ArrayList<>();
	private List<String> urlQueryValues = new ArrayList<>();
	private List<String> tags = new ArrayList<>();

	// geolocation
	private String geoHash = "";
	private String country = "";
	private double[] location = new double[2];
	private String ASN = "";

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Boolean isOutsideOfficeHours() {
		return isOutsideOfficeHours;
	}

	public void setIsOutsideOfficeHours(Boolean isOutsideOfficeHours) {
		this.isOutsideOfficeHours = isOutsideOfficeHours;
	}

	public Boolean isHostBlacklisted() {
		return isHostBlacklisted;
	}

	public void setIsHostBlacklisted(Boolean isHostBlacklisted) {
		this.isHostBlacklisted = isHostBlacklisted;
	}

	public String getipSource() {
		return ipSource;
	}

	public void setipSource(String ipSource) {
		this.ipSource = ipSource;
	}

	public String getIpTarget() {
		return ipTarget;
	}

	public void setIpTarget(String ipTarget) {
		this.ipTarget = ipTarget;
	}

	public String getHttpMethod() {
		return httpMethod;
	}

	public void setHttpMethod(String httpMethod) {
		this.httpMethod = httpMethod;
	}

	public String getHttpResponse() {
		return httpResponse;
	}

	public void setHttpResponse(String httpResponse) {
		this.httpResponse = httpResponse;
	}

	public long getRequestSize() {
		return requestSize;
	}

	public void setRequestSize(long requestSize) {
		this.requestSize = requestSize;
	}

	public long getResponseSize() {
		return responseSize;
	}

	public void setResponseSize(long responseSize) {
		this.responseSize = responseSize;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public Boolean isTunnel() {
		return isTunnel;
	}

	public void setIsTunnel(Boolean isTunnel) {
		this.isTunnel = isTunnel;
	}

	public Boolean isIp() {
		return isIp;
	}

	public void setIsIp(Boolean isIp) {
		this.isIp = isIp;
	}

	public Boolean isBadAgent() {
		return isBadAgent;
	}

	public void setIsBadAgent(Boolean isBadAgent) {
		this.isBadAgent = isBadAgent;
	}

	public Boolean isHttp10() {
		return isHttp10;
	}

	public void setIsHttp10(Boolean isHttp10) {
		this.isHttp10 = isHttp10;
	}

	public Boolean isBadCert() {
		return isBadCert;
	}

	public void setIsBadCert(Boolean isBadCert) {
		this.isBadCert = isBadCert;
	}

	public String getUrlHost() {
		return urlHost;
	}

	public void setUrlHost(String urlHost) {
		this.urlHost = urlHost;
	}

	public int getUrlPort() {
		return urlPort;
	}

	public void setUrlPort(int urlPort) {
		this.urlPort = urlPort;
	}

	public String getUrlPath() {
		return urlPath;
	}

	public void setUrlPath(String urlPath) {
		this.urlPath = urlPath;
	}

	public String getUrlScheme() {
		return urlScheme;
	}

	public void setUrlScheme(String urlScheme) {
		this.urlScheme = urlScheme;
	}

	public List<String> getUrlQueryKeys() {
		return urlQueryKeys;
	}

	public void setUrlQueryKeys(List<String> urlQueryKeys) {
		this.urlQueryKeys = urlQueryKeys;
	}

	public List<String> getUrlQueryValues() {
		return urlQueryValues;
	}

	public void setUrlQueryValues(List<String> urlQueryValues) {
		this.urlQueryValues = urlQueryValues;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getGeoHash() {
		return geoHash;
	}

	public void setGeoHash(String geoHash) {
		this.geoHash = geoHash;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public double[] getLocation() {
		return location;
	}

	public void setLocation(double[] location) {
		this.location = location;
	}

	public String getASN() {
		return ASN;
	}

	public void setASN(String ASN) {
		this.ASN = ASN;
	}
}
