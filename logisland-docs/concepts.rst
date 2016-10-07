
Core concepts
====

The main goal of LogIsland framework is to provide tools to automatically extract valuable knowledge from historical log data. To do so we need two different kind of processing over our technical stack :

1. Grab events from logs
2. Perform Event Pattern Mining (EPM)

What we know about `Log`/`Event` properties :

- they're naturally temporal
- they carry a global type (user request, error, operation, system failure...)
- they're semi-structured
- they're produced by software, so we can deduce some templates from them
- some of them are correlated
- some of them are frequent (or rare)
- some of them are monotonic
- some of them are of great interest for system operators


From raw to structure
----

The first part of the process is to extract semantics from semi-structured data such as logs. The main objective of this phase is to introduce a cannonical semantics in log data that we will call `Event` which will be easier for us to process with data mining algorithm


- log parser 
- log classification/clustering
- event generation
- event summarization

Event pattern mining
----

Once we have a cannonical semantic in the form of events we can perform time window processing over our events set. All the algorithms we can run on it will help us to find some of the following properties : 

- sequential patterns
- events burst
- frequent pattern
- rare event
- highly correlated events
- correlation between time series & events

