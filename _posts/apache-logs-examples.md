---
layout: post
title: parse apache logs
------------------------


### Launch the parser stream

## Install logIsland

get [log-island-0.9.1.tgz]({{ site.baseurl }}/public/log-island-0.9.1.tgz) 
and unzip it where you desire on your edge node 
(which should be able to communicate with a spark cluster)

define LOGISLAND_HOME as you desire, here we'll just do a no permanent export

    export LOGISLAND_HOME=path/to/unzipped-log-island

## Spark configuration

   LogIsland need spark 1.6 binaries to run. So make your SPARK_HOME
   point to at least a 1.6 spark version. Then it should work on any 
   spark version cluster.

## Launch the apache parser log

Make sure tou have SPARK_HOME well defined, logIsland will use it.
Then you can lanch the stream job as follow:

    nohup $LOGISLAND_HOME/bin/log-parser \
        --kafka-brokers sd-84196:6667 \
        --input-topics hurence_website_logs_access \
        --output-topics hurence_website_event_access \
        --max-rate-per-partition 10000 \
        --log-parser com.hurence.logisland.plugin.apache.ApacheLogParser \
        --zk-quorum zookeeperIp > hurence_website_logs_access_parser.log &
    
This job will parse logs written into the 'access_logs'
topic and inject event to the 'access_event' topic en mode 'streaming'.
    
    nohup $LOGISLAND_HOME/bin/log-parser \
        --kafka-brokers sd-84196:6667 \
        --input-topics hurence_website_logs_error \
        --output-topics hurence_website_event_error \
        --max-rate-per-partition 10000 \
        --log-parser com.hurence.logisland.plugin.apache.ApacheLogParser \
        --zk-quorum sd-76387.dedibox.fr > hurence_website_logs_error_parser.log &
    
This job will parse logs written into the 'error_logs'
topic and inject event to the 'error_event' topic en mode 'streaming'.



### Launch the event indexer stream

In our case we will use elastic search. I compiled a logIsland version with
ES 1.7.1 because I will index into an 1.7.1 ES cluster. Make sure the version
matches ! If not you may have to recompile logIsland with the right version.
We will launch a job for each of the topics we want to index but we could
specify all the topic in an unique jobs (see event-indexer job)

## access logs

    nohup $LOGISLAND_HOME/bin/event-indexer \
        --kafka-brokers sd-84196:6667 \
        --es-cluster hurence \
        --es-host sd-84186.dedibox.fr \
        --index-name apache-hurence-website-acess-log \
        --input-topics hurence_website_event_access \
        --max-rate-per-partition 10000 \
        --event-mapper com.hurence.logisland.plugin.apache.ApacheEventMapper \
        --zk-quorum sd-76387.dedibox.fr > hurence_website_event_access_indexer.log &
    
## error logs

    nohup $LOGISLAND_HOME/bin/event-indexer \
        --kafka-brokers sd-84196:6667 \
        --es-host sd-84186.dedibox.fr \
        --es-cluster hurence \
        --index-name apache-hurence-website-error-log \
        --input-topics hurence_website_event_error \
        --max-rate-per-partition 10000 \
        --event-mapper com.hurence.logisland.plugin.apache.ApacheEventMapper \
        --zk-quorum sd-76387.dedibox.fr > hurence_website_event_error_indexer.log &


### Launch logstash

verify the previous jobs are running well. Then we are ready to feed
our topics ! In my case I used this logstash conf:

    input {
      file {
        path => "/var/log/apache2/access.log"
        start_position => "beginning"
        sincedb_path => "/dev/null"
      }
      file {
        path => "/var/log/apache2/error.log"
        start_position => "beginning"
        sincedb_path => "/dev/null"
      }
    }
    
    filter {
      if [path] =~ "access" {
        mutate { replace => { "type" => "apache_access" } }
        grok {
          match => { "message" => "%{COMBINEDAPACHELOG}" }
        }
      }
      if [path] =~ "error" {
        mutate { replace => { "type" => "apache_error" } }
        grok {
          match => { "message" => "%{COMBINEDAPACHELOG}" }
        }
      }
    }
    
    output {
          if [type] == "apache_access" {
    	kafka {
                    bootstrap_servers => "sd-84196.dedibox.fr:6667"
    		topic_id => hurence_website_logs_access
            	codec => plain {
               		format => "%{message}"
            	}
          	      }
          } 
         if [type] == "apache_error" {
    	kafka {
                    bootstrap_servers => "sd-84196.dedibox.fr:6667"
    		topic_id => hurence_website_logs_error
            	codec => plain {
               		format => "%{message}"
            	}
          	      }
          }
    }
    
But you may have to make some change in order to make it works for you.
See [logstash docs](https://www.elastic.co/guide/en/logstash/current/getting-started-with-logstash.html)

Once you created your desired logstash conf file, let say you named it 'apache.conf'
You can launch the job which will feed the topic.

nohup $LOGSTASH_HOME/bin/logstash -f apache.conf > logstash_apache.log &

### Look your data !!

Your data will be indexed in a real-time flux into ES ! You can look 
at your data with pluggins of elasticsearch, kibana or others. Enjoy.