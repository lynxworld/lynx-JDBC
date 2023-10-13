# lynx-JDBC
The Lynx JDBC connector is used to connect Lynx to a relational database to support executing Cypher graph queries on it.
<!-- GETTING STARTED -->
## Getting Started
### Import

Import by Maven.
* pom.xml
  ```xml
  <dependency>
      <groupId>org.grapheco</groupId>
      <artifactId>lynx-LDBC</artifactId>
      <version>0.1</version>
  </dependency>
  ```

### How to Use

1. Connect to database.
  ```scala
  val connector: LynxJDBCConnector = LynxJDBCConnector.connect(
      "jdbc:mysql://localhost:3306/DB", //jdbc url
      "username", // username
      "password") // password
  ```
2. Execute Cypher query.
  ```scala
  connector.run("match (n) return n limit 10")
  ```

### Schema & Config
...

