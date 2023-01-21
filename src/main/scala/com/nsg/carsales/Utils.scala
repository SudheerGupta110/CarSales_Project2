package com.nsg.carsales

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.sun.xml.internal.fastinfoset.sax.Properties
import java.io.FileNotFoundException
import scala.io.Source
import java.util.Properties
import com.typesafe.config.ConfigFactory

object Utils {

  def readConfigParams() : Unit = {

    val url = getClass.getResource("application.properties")
    val properties: Properties = new Properties()

    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    else {
      logger.error("properties file cannot be loaded at path " +path)
      throw new FileNotFoundException("Properties file cannot be loaded);
    }

    val table = properties.getProperty("hbase_table_name")
    val zquorum = properties.getProperty("localhost")
    val port = properties.getProperty("2181")

    /*val config = ConfigFactory.load("C:\\Users\\Sudheer Gupta\\IdeaProjects\\CarSales_Project2\\src\\main\\scala\\com\\nsg\\carsales\\configs\\schema.conf")//.getConfig("configs.schema")
    //config.entrySet().iterator().forEachRemaining(x => println(x))

    //val sparkConfig = config.getConfig("spark")
    //val mysqlConfig = config.getConfig("mysql")
    val schemaConfig = config.getConfig("schema")
    //val appName = sparkConfig.getString(BatchConstants.GET_APP_NAME)
    val datatype = schemaConfig.getString("ID")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>"+datatype)*/
  }
}
