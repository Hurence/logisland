Query matching based on `Luwak <http://www.confluent.io/blog/real-time-full-text-search-with-luwak-and-samza/>`_

you can use this processor to handle custom events defined by lucene queries
a new record is added to output each time a registered query is matched

A query is expressed as a lucene query against a field like for example: 

.. code::

   message:'bad exception'
   error_count:[10 TO *]
   bytes_out:5000
   user_name:tom*

Please read the `Lucene syntax guide <https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description>`_ for supported operations

.. warning::
   don't forget to set numeric fields property to handle correctly numeric ranges queries
