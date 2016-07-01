/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.serializer;

import com.hurence.logisland.event.Event;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author tom
 */
public class EventAvroSerializerTest {


	@Test
	public void avroSerde() throws IOException {
		System.out.println("avroSerde");

		final Schema.Parser parser = new Schema.Parser();
		final Schema schema = parser.parse(
				new FileInputStream(EventAvroSerializerTest.class.getResource("/schemas/event.avsc").getFile()));
		final EventAvroSerializer serializer = new EventAvroSerializer(schema);


		List<String> tags = new ArrayList<>(Arrays.asList("spam","filter","mail"));
		Event event = new Event("cisco");
		event.put("timestamp", "long", new Date().getTime());
		event.put("method", "string", "GET");
		event.put("ipSource", "string", "123.34.45.123");
		event.put("ipTarget", "string", "255.255.255.255");
		event.put("urlScheme", "string", "http");
		event.put("urlHost", "string", "origin-www.20minutes.fr");
		event.put("urlPort", "string", "80");
		event.put("urlPath", "string", "/r15lgc-100KB.js");
		event.put("requestSize", "int", 1399);
		event.put("responseSize", "int", 452);
		event.put("isOutsideOfficeHours", "boolean", false);
		event.put("isHostBlacklisted", "boolean", false);
		event.put("tags", "array", tags);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(baos, event);
		baos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Event deserializedEvent = serializer.deserialize(bais);

		assertTrue(deserializedEvent.equals(event));

	}

	@Test
	public void kryoSerialisationBigEventTest() throws IOException {
		System.out.println("kryoSerialisationTest");

		final EventKryoSerializer serializer = new EventKryoSerializer(true);

		Event[] events = new Event[100];
		for (int i=0; i<100; i++) {
			Event event = new Event("mtr");
			event.put("TRACE", "java.lang.string", "Outbound Message" + "\n" +
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

			event.put("PARSING_DATESTAMP", "java.lang.long", 1453804816376L);
			event.put("PLAYER_TYPE", "java.lang.string", "PLAYER-WEB-TOTO");
			event.put("SERVER", "java.lang.string", "fl0046");
			event.put("DATESTAMP", "java.lang.long", 1447051243255L);
			event.put("SESSION", "java.lang.string", "PLAYER-WEB-TOTO:402215355:FF70Fsdf0D3D752sdf15B1ED5CB5:sdfp");
			event.put("SESSION_ID", "java.lang.string", "4022sdf5:FF70F2sdf23891715Bsdfoadp");
			event.put("FUNCTION", "java.lang.string", "mtr");
			event.put("LOG_LEVEL", "java.lang.string", "ERROR");
			events[i] = event;
		}


		for (int i=0;i<100;i++) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			serializer.serialize(baos, events[i]);
			baos.close();

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			Event deserializedEvent = serializer.deserialize(bais);

			assertTrue(deserializedEvent.equals(events[i]));
		}

	}

	@Test
	public void kryoSerialisationSmallEventTest() throws IOException {
		System.out.println("kryoSerialisationTest");

		final EventKryoSerializer serializer = new EventKryoSerializer(true);

		Event event = new Event("mtr");
		event.put("TRACE", "java.lang.string", "Outbound Message" + "\n" +
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

		event.put("PARSING_DATESTAMP", "java.lang.long", 1453804816376L);
		event.put("PLAYER_TYPE", "java.lang.string", "PLAYER-WEB-sdf");
		event.put("SERVER", "java.lang.string", "flxx46");
		event.put("DATESTAMP", "java.lang.long", 1447051243255L);
		event.put("SESSION", "java.lang.string", "PLAYER-WEB-HURENCE:40221fghfghfghfghfgh5355:fgfghfghfgjghjkh:jkljkl");
		event.put("SESSION_ID", "java.lang.string", "dfgdfgdfgdfgf:jkljkljkljkljhfghdfgdfg:oadp");
		event.put("FUNCTION", "java.lang.string", "rrr");
		event.put("LOG_LEVEL", "java.lang.string", "ERROR");


		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(baos, event);
		baos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Event deserializedEvent = serializer.deserialize(bais);

		assertTrue(deserializedEvent.equals(event));
	}
}