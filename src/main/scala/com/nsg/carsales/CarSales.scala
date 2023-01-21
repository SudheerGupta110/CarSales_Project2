package com.nsg.carsales

//import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.types.{IntegerType,StringType,LongType,DoubleType,ArrayType,StructType,StructField}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SaveMode
/**
 * Hello world!
 *
 */
object CarSales extends App {
  override def main(args: Array[String]): Unit = {

    args.foreach( x => println(x))
    Utils.readConfigParams()

    val spark = SparkSession.builder().appName("CarSales")
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.format("csv").option("header", "true")
      .load(args.head)



    val df_main = df.select("ID","Region","Price","Year","Manufacturer","Odometer")
    df_main.show()
    println(">>> record count in file::" + df_main.count())

    val df2 = spark.read.format("csv").option("header", "true")
      .load(args(1))
    val df_inc = df2.select("ID","Region","Price","Year","Manufacturer","Odometer")
    df_inc.show()
    println(">>> record count in file inc::" + df_inc.count())

    val df_new = df2.join(df_main,df_main.col("ID") === df2.col("ID"),"left_anti")
    println(">>> CDC cont:::" + df_new.count()) //new records inserted
    df_new.show()

    val df_mod = df2.join(df_main, Seq("ID","Region","Price","Year","Manufacturer","Odometer"),"left_anti")
        .join(df_new,Seq("ID"),"left_anti")
    println(">>> df_mod cont:::" + df_mod.count()) //new records inserted
    df_mod.show()

    val df_unmod = df_main.join(df_mod, Seq("ID"),"left_anti")
    println(">>> df_unmod cont:::" + df_unmod.count()) //new records inserted
    df_unmod.show()

    val final_df = df_unmod.unionByName(df_mod).unionByName(df_new)
    println(">>> final_df cont:::" + final_df.count()) //new records inserted
    final_df.show()

    /*val connString = ""
    val user = ""
    val password = "
    val table = ""

    val prop=new java.util.Properties()
    prop.put("user",user)
    prop.put("password",password)
    val url=connString
    //df is a dataframe contains the data which you want to write.
    df.write.mode(SaveMode.Append).jdbc(url,table,prop)
  */






    /*val outputSchema =
      StructType(
        Array(
          StructField("ID", IntegerType, nullable=false),
          StructField("ListingUrl", StringType, nullable=false),
          StructField("Region", LongType, nullable=false),
          StructField("RegionUrl", LongType, nullable=false),
          StructField("Price", LongType, nullable=false),
          StructField("Year", LongType, nullable=false),
          StructField("Manufacturer", DoubleType),
          StructField("Model", DoubleType),
          StructField("Condition", DoubleType),

          StructField("Cylinders", LongType, nullable=false),
          StructField("Fuel", LongType, nullable=false),
          StructField("Odometer", DoubleType),
          StructField("Title_Status", DoubleType),
          StructField("Transmission", DoubleType),

          StructField("topValues", ArrayType(StructType(Array(
            StructField("value", StringType),
            StructField("count", LongType)
          ))))
        )
      )
    */
  }

}
