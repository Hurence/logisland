/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.record.serializer;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author tom
 */
public class AvroRecordSerializerTest {


	@Test
	public void avroSerde() throws IOException {
		System.out.println("avroSerde");

		final Schema.Parser parser = new Schema.Parser();
		final Schema schema = parser.parse(
				new FileInputStream(AvroRecordSerializerTest.class.getResource("/schemas/event.avsc").getFile()));
		final AvroRecordSerializer serializer = new AvroRecordSerializer(schema);

		Record record = new Record("cisco");
		record.setId("firewall_record1");
		record.setField("method", FieldType.STRING, "GET");
		record.setField("ip_source", FieldType.STRING, "123.34.45.123");
		record.setField("ip_target", FieldType.STRING, "255.255.255.255");
		record.setField("url_scheme", FieldType.STRING, "http");
		record.setField("url_host", FieldType.STRING, "origin-www.20minutes.fr");
		record.setField("url_port", FieldType.STRING, "80");
		record.setField("url_path", FieldType.STRING, "/r15lgc-100KB.js");
		record.setField("request_size", FieldType.INT, 1399);
		record.setField("response_size", FieldType.INT, 452);
		record.setField("is_outside_office_hours", FieldType.BOOLEAN, false);
		record.setField("is_host_blacklisted", FieldType.BOOLEAN, false);
		record.setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(baos, record);
		baos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Record deserializedRecord = serializer.deserialize(bais);

		assertTrue(deserializedRecord.equals(record));

	}

	@Test
	public void kryoSerialisationBigEventTest() throws IOException {
		System.out.println("kryoSerialisationTest");

		final KryoRecordSerializer serializer = new KryoRecordSerializer(true);

		Record[] records = new Record[100];
		for (int i=0; i<100; i++) {
			Record record = new Record("mtr");
			record.setStringField("TRACE", "Outbound Message" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
					"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.servlet.ServletController.invokeDestination(ServletController.java:223) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.http.AbstractHTTPDestination.invoke(AbstractHTTPDestination.java:251) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.transport.ChainInitiationObserver.onMessage(ChainInitiationObserver.java:121) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:307) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor.handleMessage(ServiceInvokerInterceptor.java:96) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.interceptor.ServiceInvokerInterceptor$1.run(ServiceInvokerInterceptor.ja va:59) [cxf-core-3.0.3.jar:3.0.3]" + "\n" +
					"at org.apache.cxf.jaxrs.JAXRSInvoker.invoke(JAXRSInvoker.java:99) [cxf-rt-frontend-jaxrs-3.0.3.jar:3.0.3");

			record.setField("PARSING_DATESTAMP", FieldType.LONG, 1453804816376L);
			record.setStringField("PLAYER_TYPE", "PLAYER-WEB-TOTO");
			record.setStringField("SERVER", "fl0046");
			record.setField("DATESTAMP", FieldType.LONG, 1447051243255L);
			record.setStringField("SESSION", "PLAYER-WEB-TOTO:402215355:FF70Fsdf0D3D752sdf15B1ED5CB5:sdfp");
			record.setStringField("SESSION_ID", "4022sdf5:FF70F2sdf23891715Bsdfoadp");
			record.setStringField("FUNCTION", "mtr");
			record.setStringField("LOG_LEVEL", "ERROR");
			records[i] = record;
		}


		for (int i=0;i<100;i++) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			serializer.serialize(baos, records[i]);
			baos.close();

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			Record deserializedRecord = serializer.deserialize(bais);

			assertTrue(deserializedRecord.equals(records[i]));
		}

	}

	@Test
	public void kryoSerialisationSmallEventTest() throws IOException {
		System.out.println("kryoSerialisationTest");

		final KryoRecordSerializer serializer = new KryoRecordSerializer(true);

		Record record = new Record("mtr");
		record.setStringField("TRACE", "Outbound Message" + "\n" +
				"at java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]" + "\n" +
				"at org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:61) [tomcat-coyote.jar:7.0.55]" + "\n" +
				"at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615) [na:1.7 .0_80]" + "\n" +
				"at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145) [na:1.7.0_80]" + "\n" +
				"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.run(NioEndpoint.java:1695) [tomcat-coyote.jar:7.0.55]" + "\n" +
				"at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736) [tomcat-coyote.jar:7.0.55]" + "\n" +
				"at org.apache.coyote.AbstractProtocol$AbstractConnectionHandler.process(AbstractProtocol.j ava:611) [tomcat-coyote.jar:7.0.55]" + "\n" +
				"at org.apache.coyote.http11.AbstractHttp11Processor.process(AbstractHttp11Processor.java:1070) [tomcat-coyote.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:408) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:116) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.valves.AccessLogValve.invoke(AccessLogValve.java:950) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:103) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:171) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:501)[catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:122) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:220) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:52) [tomcat7-websocket.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:208) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:241) [catalina.jar:7.0.55]" + "\n" +
				"at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:107) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
				"at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:85) [spring-web-4.1.6.RELEASE.jar:4.1.6.RELEASE]" + "\n" +
				"at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:20 8) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:303) [catalina.jar:7.0.55]" + "\n" +
				"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.service(AbstractHTTPServlet.java:265) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
				"at javax.servlet.http.HttpServlet.service(HttpServlet.java:649) [servlet-api.jar:na]" + "\n" +
				"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.doPut(AbstractHTTPServlet.java:226) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
				"at org.apache.cxf.transport.servlet.AbstractHTTPServlet.handleRequest(AbstractHTTPServlet.java:290) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
				"at org.apache.cxf.transport.servlet.CXFNonSpringServlet.invoke(CXFNonSpringServlet.java:171) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
				"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:149) [cxf-rt-transports-http-3.0.3.jar:3.0.3]" + "\n" +
				"at org.apache.cxf.transport.servlet.ServletController.invoke(ServletController.java:197) [cxf-rt-transports-http-3.0.3.jar:3.0.3]");

		record.setField("PARSING_DATESTAMP", FieldType.LONG, 1453804816376L);
		record.setStringField("PLAYER_TYPE", "PLAYER-WEB-sdf");
		record.setStringField("SERVER",  "flxx46");
		record.setField("DATESTAMP", FieldType.LONG, 1447051243255L);
		record.setStringField("SESSION",  "PLAYER-WEB-HURENCE:40221fghfghfghfghfgh5355:fgfghfghfgjghjkh:jkljkl");
		record.setStringField("SESSION_ID",  "dfgdfgdfgdfgf:jkljkljkljkljhfghdfgdfg:oadp");
		record.setStringField("FUNCTION",  "rrr");
		record.setStringField("LOG_LEVEL",  "ERROR");


		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(baos, record);
		baos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Record deserializedRecord = serializer.deserialize(bais);

		assertTrue(deserializedRecord.equals(record));
	}
}