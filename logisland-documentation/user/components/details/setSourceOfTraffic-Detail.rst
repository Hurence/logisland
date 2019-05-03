Compute the source of traffic of a web session. Users arrive at a website or application through a variety of sources, 
including advertising/paying campaigns, search engines, social networks, referring sites or direct access. 
When analysing user experience on a webshop, it is crucial to collect, process, and report the campaign and traffic-source data. 
To compute the source of traffic of a web session, the user has to provide the utm_* related properties if available
i-e: **utm_source.field**, **utm_medium.field**, **utm_campaign.field**, **utm_content.field**, **utm_term.field**)
, the referer (**referer.field** property) and the first visited page of the session (**first.visited.page.field** property).
By default the source of traffic information are placed in a flat structure (specified by the **source_of_traffic.suffix** property
with a default value of source_of_traffic). To work properly the setSourceOfTraffic processor needs to have access to an 
Elasticsearch index containing a list of the most popular search engines and social networks. The ES index (specified by the **es.index** property) should be structured such that the _id of an ES document MUST be the name of the domain. If the domain is a search engine, the related ES doc MUST have a boolean field (default being search_engine) specified by the property **es.search_engine.field** with a value set to true. If the domain is a social network , the related ES doc MUST have a boolean field (default being social_network) specified by the property **es.social_network.field** with a value set to true. 
