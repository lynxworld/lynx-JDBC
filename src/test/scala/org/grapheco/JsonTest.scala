package org.grapheco

import org.grapheco.schema.SchemaManager
import org.junit.jupiter.api.Test

import java.sql.DriverManager

@Test
class JsonTest {

  @Test
  def saveJson(): Unit = {
    SchemaManager.saveJson("", schema = SchemaManager.autoGeneration(
      DriverManager.getConnection(
        "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false",
        "root", "Hc1478963!")
    ))
  }
}
