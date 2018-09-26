Plugins
=======

In this chapter we will present you how the logisland plugins architecture and how to manage them

.. contents:: Table of Contents


What's a plugin?
----------------

A logisland plugin is anything can bring a functionality to logisland.

It can be:

* A processor
* A controller service
* A connector


How a plugin is packaged
------------------------

A plugin is a jar in the logisland lib folder containing a special manifest giving some information about:

* Exported components
* Versions
* Classloading rules

As well a plugin jar contains every additional dependency is required to make it work with logisland, that ensures the portability with a single file.

How about naming?
-----------------

When talking about a plugin we talk about an *artifact*.

Logisland uses the same maven naming convention (groupId, artifactId, version) to locate a plugin.
This ensure a component to be unique and versioned.


Getting started
---------------

Everything about plugins is managed through the *components.sh* client utility (in the bin folder along with logisland.sh command).

Let's see the main actions you can do with

List all components
___________________

Simply use the *-l* option.

.. code-block:: sh

  bin/components.sh -l

  Listing details for 1 installed modules.
  Artifact: com.hurence.logisland:logisland-processor-common:1.0.0
  Name: Common processors bundle
  Version: 1.0.0
  Components provided:
	com.hurence.logisland.processor.AddFields
	com.hurence.logisland.processor.ApplyRegexp
	com.hurence.logisland.processor.ConvertFieldsType
	com.hurence.logisland.processor.DebugStream
	com.hurence.logisland.processor.EvaluateJsonPath
	com.hurence.logisland.processor.FilterRecords
	com.hurence.logisland.processor.FlatMap
	com.hurence.logisland.processor.GenerateRandomRecord
	com.hurence.logisland.processor.ModifyId
	com.hurence.logisland.processor.NormalizeFields
	com.hurence.logisland.processor.ParseProperties
	com.hurence.logisland.processor.RemoveFields
	com.hurence.logisland.processor.SelectDistinctRecords
	com.hurence.logisland.processor.SendMail
	com.hurence.logisland.processor.SplitField
	com.hurence.logisland.processor.SplitText
	com.hurence.logisland.processor.SplitTextMultiline
	com.hurence.logisland.processor.SplitTextWithProperties
	com.hurence.logisland.processor.alerting.CheckAlerts
	com.hurence.logisland.processor.alerting.CheckThresholds
	com.hurence.logisland.processor.alerting.ComputeTags
	com.hurence.logisland.processor.datastore.BulkPut
	com.hurence.logisland.processor.datastore.EnrichRecords
	com.hurence.logisland.processor.datastore.MultiGet


This above is the logisland common processor modules bundled by default in the distribution.

As we can see the command line tell us some nice information:

* The file name
* The version
* The components it provides


Install a component
___________________

You can install two things of components:

* A logisland plugin
* A kafka connect source or sink (more information on `connectors section <connectors.rst>`_)


The generic syntax for both is:


.. code-block:: sh

  bin/components.sh -i <plugin_artifact>


For instance, if we want to install elasticsearch 5.4 controller service we are going to install the related module called *com.hurence.logisland:logisland-service-elasticsearch_5_4_0-client:<logisland_version>*

.. code-block:: sh

  bin/components.sh -i com.hurence.logisland:logisland-service-elasticsearch_5_4_0-client:1.0.0

  Downloading dependencies. Please hold on...

  Found logisland plugin Elasticsearch 5.4.0 Service Plugin version 0.15.0

  It will provide:
	com.hurence.logisland.service.elasticsearch.Elasticsearch_5_4_0_ClientService

  Install done!


Remove a component
__________________

Just delete the jar on the lib folder or use the components.sh with the -r option.

Example

.. code-block:: sh

  bin/components.sh -i com.hurence.logisland:logisland-service-elasticsearch_5_4_0-client:1.0.0


Which module contains my component?
-----------------------------------

You can easily know with module you require to install in case you need a specific component.

The `component documentation <components.rst>`_ contains a *Module* section for each component. It will tell you the artifact you should install.



How about the distribution?
---------------------------

Logisland uses `apache ivy <http://ant.apache.org/ivy/>`_ to download the plugins. This allows you to choose the right repository (e.g. a common nexus or an enterprise artifactory) in order to manage and control the dependencies.

You can fine tune this by editing (at your own risks) the ivy.xml file on the conf directory.


