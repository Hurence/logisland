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
package com.hurence.logisland.service.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

/**
* A JUnit rule which starts an embedded opendsitro elasticsearch docker container to test security features
*/
public class ESOpenDistroRule implements TestRule {

   /**
    * The internal-transport client that talks to the local node.
    */
   private RestHighLevelClient client;
   private ElasticsearchOpenDistroContainer container;
   private String opendistroUsername;
   private String opendistroPassword;

    private static Logger logger = LoggerFactory.getLogger(ESOpenDistroRule.class);

    public ESOpenDistroRule(String opendistroUsername, String opendistroPassword) {
        this.opendistroUsername = opendistroUsername;
        this.opendistroPassword = opendistroPassword;
    }

    /**
    * Return a closure which starts an embedded ES OpenDistro docker container, executes the unit-test, then shuts down the
    * ES instance.
    */
   @Override
   public Statement apply(Statement base, Description description) {
       return new Statement() {
           @Override
           public void evaluate() throws Throwable {
               container = new ElasticsearchOpenDistroContainer("amazon/opendistro-for-elasticsearch:1.4.0",
                       opendistroUsername, opendistroPassword);
               container.start();

               // TODO: if testcontainers support no SSL server validation one can use the wait strategy
               // in ElasticsearchOpenDistroContainer instead. See inside ElasticsearchOpenDistroContainer.
               long wait = 10000L;
               logger.info("Waiting for ES open distro container to start for " + wait/1000 + " seconds");
               Thread.sleep(wait);

               /**
                * Inspired from https://github.com/opendistro-for-elasticsearch/community/issues/64
                */

               RestClientBuilder builder = RestClient.builder(
                       new HttpHost(container.getHostAddress(), container.getPort(), "https"))
                       .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                           @Override
                           public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

                               // Set user/password basic auth credentials
                               final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                               credentialsProvider.setCredentials(AuthScope.ANY,
                                       new UsernamePasswordCredentials(opendistroUsername, opendistroPassword));
                               httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

                               // Set SSL trust manager and context
                               // Create and use a trust manager accepting all server certificates
                               TrustManager[] acceptAllTrustManager = new TrustManager[] { new X509TrustManager() {
                                   public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                                       return null;
                                   }
                                   public void checkClientTrusted(X509Certificate[] certs, String authType) {
                                   }

                                   public void checkServerTrusted(X509Certificate[] certs, String authType) {
                                   }
                               } };

                               SSLContext sslContext = null;
                               try {
                                   sslContext = SSLContext.getInstance("SSL");
                                   sslContext.init(null, acceptAllTrustManager, new java.security.SecureRandom());
                               } catch (Exception e) {
                                   e.printStackTrace();
                                   throw new RuntimeException(e);
                               }
                               httpClientBuilder.setSSLContext(sslContext);

                               return httpClientBuilder;
                           }
                       });
               client = new RestHighLevelClient(builder);

               try {
                   base.evaluate(); // execute the unit test
               } finally {
                   client.close();
                   container.stop();
               }
           }
       };
   }

    public String getHostPortString() {
        return container.getHostPortString();
    }

    public String getHostAddress() {
        return container.getHostAddress();
    }

    public int getPort() {
        return container.getPort();
    }

   /**
    * Return the object through which operations can be performed on the ES cluster.
    */
   public RestHighLevelClient getClient() {
       return client;
   }

}