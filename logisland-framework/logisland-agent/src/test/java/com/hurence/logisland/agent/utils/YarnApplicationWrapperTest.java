package com.hurence.logisland.agent.utils;

import org.junit.Assert;
import org.junit.Test;


public class YarnApplicationWrapperTest {

    private static final String sample = "17/03/15 10:46:03 INFO impl.TimelineClientImpl: Timeline service address: http://sd-79372.dedibox.fr:8188/ws/v1/timeline/\n" +
            "17/03/15 10:46:03 INFO client.AHSProxy: Connecting to Application History server at sd-79372.dedibox.fr/10.91.58.228:10200\n" +
            "17/03/15 10:46:04 WARN ipc.Client: Failed to connect to server: sd-79372.dedibox.fr/10.91.58.228:8032: retries get failed due to exceeded maximum allowed retries number: 0\n" +
            "java.net.ConnectException: Connection refused\n" +
            "\tat sun.nio.ch.SocketChannelImpl.checkConnect(Native Method)\n" +
            "\tat sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:717)\n" +
            "\tat org.apache.hadoop.net.SocketIOWithTimeout.connect(SocketIOWithTimeout.java:206)\n" +
            "\tat org.apache.hadoop.net.NetUtils.connect(NetUtils.java:531)\n" +
            "\tat org.apache.hadoop.net.NetUtils.connect(NetUtils.java:495)\n" +
            "\tat org.apache.hadoop.ipc.Client$Connection.setupConnection(Client.java:650)\n" +
            "\tat org.apache.hadoop.ipc.Client$Connection.setupIOstreams(Client.java:745)\n" +
            "\tat org.apache.hadoop.ipc.Client$Connection.access$3200(Client.java:397)\n" +
            "\tat org.apache.hadoop.ipc.Client.getConnection(Client.java:1618)\n" +
            "\tat org.apache.hadoop.ipc.Client.call(Client.java:1449)\n" +
            "\tat org.apache.hadoop.ipc.Client.call(Client.java:1396)\n" +
            "\tat org.apache.hadoop.ipc.ProtobufRpcEngine$Invoker.invoke(ProtobufRpcEngine.java:233)\n" +
            "\tat com.sun.proxy.$Proxy17.getApplications(Unknown Source)\n" +
            "\tat org.apache.hadoop.yarn.api.impl.pb.client.ApplicationClientProtocolPBClientImpl.getApplications(ApplicationClientProtocolPBClientImpl.java:251)\n" +
            "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
            "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n" +
            "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n" +
            "\tat java.lang.reflect.Method.invoke(Method.java:497)\n" +
            "\tat org.apache.hadoop.io.retry.RetryInvocationHandler.invokeMethod(RetryInvocationHandler.java:278)\n" +
            "\tat org.apache.hadoop.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:194)\n" +
            "\tat org.apache.hadoop.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:176)\n" +
            "\tat com.sun.proxy.$Proxy18.getApplications(Unknown Source)\n" +
            "\tat org.apache.hadoop.yarn.client.api.impl.YarnClientImpl.getApplications(YarnClientImpl.java:484)\n" +
            "\tat org.apache.hadoop.yarn.client.cli.ApplicationCLI.listApplications(ApplicationCLI.java:401)\n" +
            "\tat org.apache.hadoop.yarn.client.cli.ApplicationCLI.run(ApplicationCLI.java:207)\n" +
            "\tat org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:76)\n" +
            "\tat org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:90)\n" +
            "\tat org.apache.hadoop.yarn.client.cli.ApplicationCLI.main(ApplicationCLI.java:83)\n" +
            "17/03/15 10:46:04 INFO client.ConfiguredRMFailoverProxyProvider: Failing over to rm2\n" +
            "Total number of applications (application-types: [] and states: [SUBMITTED, ACCEPTED, RUNNING]):2\n" +
            "                Application-Id\t    Application-Name\t    Application-Type\t      User\t     Queue\t             State\t       Final-State\t       Progress\t                       Tracking-URL\n" +
            "application_1484246503127_0024\t          SaveToHDFS\t               SPARK\t   hurence\t   default\t           RUNNING\t         UNDEFINED\t            10%\t           http://10.91.84.219:4051\n" +
            "application_1489079367586_0017\t IndexApacheLogsDemo\t               SPARK\t   hurence\t   default\t           RUNNING\t         UNDEFINED\t            10%\t           http://10.91.84.214:4050";


    @Test
    public void getApplication() throws Exception {
        YarnApplicationWrapper wrapper = new YarnApplicationWrapper(sample);

        YarnApplication app = wrapper.getApplication("SaveToHDFS");
        Assert.assertEquals(app.getId(), "application_1484246503127_0024");
        Assert.assertEquals(app.getName(), "SaveToHDFS");
        Assert.assertEquals(app.getType(), "SPARK");
        Assert.assertEquals(app.getUser(), "hurence");
        Assert.assertEquals(app.getYarnQueue(), "default");
        Assert.assertEquals(app.getState(), "RUNNING");
        Assert.assertEquals(app.getFinalState(), "UNDEFINED");
        Assert.assertEquals(app.getProgress(), "10%");
        Assert.assertEquals(app.getTrackingUrl(), "http://10.91.84.219:4051");

        YarnApplication app2 = wrapper.getApplication("IndexApacheLogsDemo");
        Assert.assertEquals(app2.getId(), "application_1489079367586_0017");
        Assert.assertEquals(app2.getName(), "IndexApacheLogsDemo");
        Assert.assertEquals(app2.getType(), "SPARK");
        Assert.assertEquals(app2.getUser(), "hurence");
        Assert.assertEquals(app2.getYarnQueue(), "default");
        Assert.assertEquals(app2.getState(), "RUNNING");
        Assert.assertEquals(app2.getFinalState(), "UNDEFINED");
        Assert.assertEquals(app2.getProgress(), "10%");
        Assert.assertEquals(app2.getTrackingUrl(), "http://10.91.84.214:4050");
    }

}