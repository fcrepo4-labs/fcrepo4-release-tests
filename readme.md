A set of integration tests that runs agains a given version of fcrepo-webapp, fcrepo-message-consumer using
a stock version of fuseki.

To run the tests: (defaulting to jetty7x)

mvn clean install -Dfcrepo.version=<version-to-test>

To run the tests using tomcat6:
mvn clean install -Dfcrepo.version=<version-to-test> -P tomcat6x