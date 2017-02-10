
Developer Guide
============

Here is a set of information which you should read if you want to contribute to the project.


TimeZone in Tests
-----------------

Your environment jdk can be different than travis ones. Be aware that there is changes on TimeZone objects between different
version of jdk... Even between 8.x.x versions.
For example TimeZone "America/Cancun" may not give the same date in your environment than in travis one...



Before request a pull request
-----------------------------

    You should do a "mvn install". It can generate some docs depending on the modifications you did.
If this generates new documents, add them before you do your pull request.



