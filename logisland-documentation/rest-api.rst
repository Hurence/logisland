Logisland REST API
==================

**The Logisland REST API for third party applications.**


.. toctree::
:maxdepth: 3

------------
Introduction
------------
Logisland makes available a standard RESTful API definition to interoperate with any third party application implementing it.

The API should be implemented by a third party application and logisland will regularly poll this endpoint in order to:

- Ask for configuration changes to be triggered.
- Report the latest configuration applied (to ease up resynchronization and business continuity).


Both flows can hence be resumed by the following sequence diagram:

.. image:: /_static/logisland_api_flows.png



------------
Usage
------------

In terms of API, two degrees of freedom are possible:

- **Dataflow**:

    A dataflow is a set of services and streams allowing a data flowing from one or more sources, being transformed and reach one or more destinations (sinks).

    Act at dataflow level if you want to:

    - Add/Remove any streaming endpoint
    - Change any active stream configuration (e.g. kafka topic)
    - Create/Remote/Modify any service


- **Pipeline**:

    A pipeline is a processing chain acting on a data flowing point-to-point.

    The api gives you the possibility to have a finer-grained control of what is going of any stream pipeline without perturbing the stream itself.
    This means that the processor chain will be dynamically reconfigured without the need of stopping the stream and reconfigure the whole dataflow.

    Act at pipeline level if you want to:

    - Add/Remove processors in the pipeline

    - Change any processor configuration

.. hint:: As a general rule, the changes will be triggered if the *lastUpdated* field of the object you are going to modify is fresher than the one known by logisland.


-----------------
API Specification
-----------------

This section resumes the Rest API specification. More details are available on the `swagger spec </_static/api.yaml>`_.

==========
Operations
==========

GET ``/dataflows/{dataflowName}``
---------------------------------


Summary
+++++++

Retrieves the configuration for a specified dataflow

Description
+++++++++++

.. raw:: html

    A dataflow is a set of services and streams allowing a data flowing from one or more sources, being transformed by a pipeline and reach one or more destinations (sinks).
Logisland will call this endpoint to know which configuration should be run.

 This endpoint also supports HTTP caching (Last-Updated, If-Modified-Since) as per RFC 7232, section 3.3

Parameters
++++++++++

.. csv-table::
:delim: |
    :header: "Name", "Located in", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 15, 10, 10, 10, 20, 30

            dataflowName | path | Yes | string |  |  | the dataflow name (aka the logisland job name)


Request
+++++++


Headers
^^^^^^^

.. code-block:: javascript

    If-Modified-Since: Timestamp of last response


Responses
+++++++++

**200**
^^^^^^^

Return the dataflow configuration.
On logisland side, the following will happen:
- At dataflow level:

  - Fully reconfigure a dataflow (stop and then start) if nothing is running (initial state) or if lastUpdated is fresher than the one of the already running dataflow.

    In this case be aware that old stream and services will be destroyed and
    new ones will be created.

  - Do nothing otherwise (keep running the active dataflow)

- At pipeline level:

  - The processor chain will be fully reconfigured if and only if the pipeline lastUpdated is fresher than the lastUpdated known by the system.

  In any case the stream is never stopped.


Type: :ref:`Versioned <d_bcefda54d79a3bedfa83231aed8d38b1>` extended :ref:`inline <i_ae1.4.115667b75409cb3251ba13c032>`

**Example:**

.. code-block:: javascript

    {
        "lastModified": "2015-01-01T15:00:00.000Z",
        "modificationReason": "somestring",
        "services": [
            {
                "component": "somestring",
                "config": [
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    },
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    }
                ],
                "documentation": "somestring",
                "name": "somestring"
            },
            {
                "component": "somestring",
                "config": [
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    },
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    }
                ],
                "documentation": "somestring",
                "name": "somestring"
            }
        ],
        "streams": [
            {
                "component": "somestring",
                "config": [
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    },
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    }
                ],
                "documentation": "somestring",
                "name": "somestring",
                "pipeline": {
                    "lastModified": "2015-01-01T15:00:00.000Z",
                    "modificationReason": "somestring",
                    "processors": [
                        {
                            "component": "somestring",
                            "config": [
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                },
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                }
                            ],
                            "documentation": "somestring",
                            "name": "somestring"
                        },
                        {
                            "component": "somestring",
                            "config": [
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                },
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                }
                            ],
                            "documentation": "somestring",
                            "name": "somestring"
                        }
                    ]
                }
            },
            {
                "component": "somestring",
                "config": [
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    },
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    }
                ],
                "documentation": "somestring",
                "name": "somestring",
                "pipeline": {
                    "lastModified": "2015-01-01T15:00:00.000Z",
                    "modificationReason": "somestring",
                    "processors": [
                        {
                            "component": "somestring",
                            "config": [
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                },
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                }
                            ],
                            "documentation": "somestring",
                            "name": "somestring"
                        },
                        {
                            "component": "somestring",
                            "config": [
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                },
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                }
                            ],
                            "documentation": "somestring",
                            "name": "somestring"
                        }
                    ]
                }
            }
        ]
    }

**304**
^^^^^^^

Nothing has been modified since the last call.

In this case the body content will be completely ignored
(hence the server can answer with an empty body to save network and resources).



**404**
^^^^^^^

Not found (the server probably does not handle this dataflow)


**default**
^^^^^^^^^^^

Unexpected error



POST ``/dataflows/{dataflowName}``
----------------------------------


Summary
+++++++

Push the configuration of running dataflows.

Description
+++++++++++

.. raw:: html

    In order to ensure business continuity, Logisland will contact the third party application in order to push a snapshot of the current configuration.
The endpoint will be called:
- On a regular basis (according to logisland configuration).
- Each time the a dataflow or a pipeline configuration change has been applied.

This service can be seen as well as a liveness ping.

Parameters
++++++++++

.. csv-table::
:delim: |
    :header: "Name", "Located in", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 15, 10, 10, 10, 20, 30

            jobId | path | Yes | string |  |  | logisland job id (aka the engine name)
            dataflowName | path | Yes | string |  |  | the dataflow name (aka the logisland job name)


Request
+++++++



.. _d_68b618b2088b15f9f9f912df4be811df:

Body
^^^^

A streaming pipeline.

:ref:`Versioned <d_bcefda54d79a3bedfa83231aed8d38b1>` extended :ref:`inline <i_ae1.4.115667b75409cb3251ba13c032>`

.. _i_ae1.4.115667b75409cb3251ba13c032:

**Inline schema:**


.. csv-table::
:delim: |
    :header: "Name", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 10, 15, 15, 30, 25

            lastModified | Yes | string | date-time |  | the last modified timestamp of this pipeline (used to trigger changes).
            modificationReason | No | string |  |  | Can be used to document latest changeset.
            services | No | array of :ref:`Component <d_3c2b4cd64485b5f73be7a1facba6ed8c>` |  |  | The service controllers.
        streams | No | array of :ref:`Component <d_3c2b4cd64485b5f73be7a1facba6ed8c>` extended :ref:`inline <i_09545770fbf157c057309e15e402b2f4>` |  |  | The engine properties.

.. code-block:: javascript

    {
        "lastModified": "2015-01-01T15:00:00.000Z",
        "modificationReason": "somestring",
        "services": [
            {
                "component": "somestring",
                "config": [
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    },
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    }
                ],
                "documentation": "somestring",
                "name": "somestring"
            },
            {
                "component": "somestring",
                "config": [
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    },
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    }
                ],
                "documentation": "somestring",
                "name": "somestring"
            }
        ],
        "streams": [
            {
                "component": "somestring",
                "config": [
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    },
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    }
                ],
                "documentation": "somestring",
                "name": "somestring",
                "pipeline": {
                    "lastModified": "2015-01-01T15:00:00.000Z",
                    "modificationReason": "somestring",
                    "processors": [
                        {
                            "component": "somestring",
                            "config": [
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                },
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                }
                            ],
                            "documentation": "somestring",
                            "name": "somestring"
                        },
                        {
                            "component": "somestring",
                            "config": [
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                },
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                }
                            ],
                            "documentation": "somestring",
                            "name": "somestring"
                        }
                    ]
                }
            },
            {
                "component": "somestring",
                "config": [
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    },
                    {
                        "key": "somestring",
                        "type": "string",
                        "value": "somestring"
                    }
                ],
                "documentation": "somestring",
                "name": "somestring",
                "pipeline": {
                    "lastModified": "2015-01-01T15:00:00.000Z",
                    "modificationReason": "somestring",
                    "processors": [
                        {
                            "component": "somestring",
                            "config": [
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                },
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                }
                            ],
                            "documentation": "somestring",
                            "name": "somestring"
                        },
                        {
                            "component": "somestring",
                            "config": [
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                },
                                {
                                    "key": "somestring",
                                    "type": "string",
                                    "value": "somestring"
                                }
                            ],
                            "documentation": "somestring",
                            "name": "somestring"
                        }
                    ]
                }
            }
        ]
    }

Responses
+++++++++

**default**
^^^^^^^^^^^

The server should return HTTP 200 OK.
By the way, the response is ignored by Logisland since the operation
has a *fire and forget* nature.


===============
Data Structures
===============

.. _d_3c2b4cd64485b5f73be7a1facba6ed8c:

Component Model Structure
-------------------------

.. csv-table::
:delim: |
    :header: "Name", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 10, 15, 15, 30, 25

            component | Yes | string |  |  |
            config | No | array of :ref:`Property <d_28dca67a05e18e9f96317b5bef61d056>` |  |  |
        documentation | No | string |  |  |
        name | Yes | string |  |  |

.. _d_68b618b2088b15f9f9f912df4be811df:

DataFlow Model Structure
------------------------

A streaming pipeline.

:ref:`Versioned <d_bcefda54d79a3bedfa83231aed8d38b1>` extended :ref:`inline <i_ae1.4.115667b75409cb3251ba13c032>`

.. _i_ae1.4.115667b75409cb3251ba13c032:

**Inline schema:**


.. csv-table::
:delim: |
    :header: "Name", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 10, 15, 15, 30, 25

            lastModified | Yes | string | date-time |  | the last modified timestamp of this pipeline (used to trigger changes).
            modificationReason | No | string |  |  | Can be used to document latest changeset.
            services | No | array of :ref:`Component <d_3c2b4cd64485b5f73be7a1facba6ed8c>` |  |  | The service controllers.
        streams | No | array of :ref:`Component <d_3c2b4cd64485b5f73be7a1facba6ed8c>` extended :ref:`inline <i_09545770fbf157c057309e15e402b2f4>` |  |  | The engine properties.

.. _d_0752e439d11.4.1d4f6b437e63ea7248:

Pipeline Model Structure
------------------------

Tracks stream processing pipeline configuration

:ref:`Versioned <d_bcefda54d79a3bedfa83231aed8d38b1>` extended :ref:`inline <i_f3879f767282c180c5b651f138c40b05>`

.. _i_f3879f767282c180c5b651f138c40b05:

**Inline schema:**


.. csv-table::
:delim: |
    :header: "Name", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 10, 15, 15, 30, 25

            lastModified | Yes | string | date-time |  | the last modified timestamp of this pipeline (used to trigger changes).
            modificationReason | No | string |  |  | Can be used to document latest changeset.
            processors | No | array of :ref:`Component <d_3c2b4cd64485b5f73be7a1facba6ed8c>` |  |  |

.. _d_865032b24aeb47b5fd3a07f7e49d88fd:

Processor Model Structure
-------------------------

A logisland 'processor'.

:ref:`Component <d_3c2b4cd64485b5f73be7a1facba6ed8c>`

.. _d_28dca67a05e18e9f96317b5bef61d056:

Property Model Structure
------------------------

.. csv-table::
:delim: |
    :header: "Name", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 10, 15, 15, 30, 25

            key | Yes | string |  |  |
            type | No | string |  | {'default': 'string'} |
            value | Yes | string |  |  |

.. _d_35858dd5b9e97d51acc7f109ceb3deb0:

Service Model Structure
-----------------------

A logisland 'controller service'.

:ref:`Component <d_3c2b4cd64485b5f73be7a1facba6ed8c>`

.. _d_ab44feb101835c4602a49f15a25615a8:

Stream Model Structure
----------------------

:ref:`Component <d_3c2b4cd64485b5f73be7a1facba6ed8c>` extended :ref:`inline <i_09545770fbf157c057309e15e402b2f4>`

.. _i_09545770fbf157c057309e15e402b2f4:

**Inline schema:**


.. csv-table::
:delim: |
    :header: "Name", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 10, 15, 15, 30, 25

            component | Yes | string |  |  |
            config | No | array of :ref:`Property <d_28dca67a05e18e9f96317b5bef61d056>` |  |  |
        documentation | No | string |  |  |
        name | Yes | string |  |  |
        pipeline | No | :ref:`Versioned <d_bcefda54d79a3bedfa83231aed8d38b1>` extended :ref:`inline <i_f3879f767282c180c5b651f138c40b05>` |  |  |

.. _d_bcefda54d79a3bedfa83231aed8d38b1:

Versioned Model Structure
-------------------------

a versioned component

.. csv-table::
:delim: |
    :header: "Name", "Required", "Type", "Format", "Properties", "Description"
        :widths: 20, 10, 15, 15, 30, 25

            lastModified | Yes | string | date-time |  | the last modified timestamp of this pipeline (used to trigger changes).
            modificationReason | No | string |  |  | Can be used to document latest changeset.

