package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val people = spark.read.json("data/input/demographie_par_commune.json")

    //How many people have France ?
    println("How many people have France ?")
    people.select(sum($"population")).show()

    //What are the top highly populated departments in France ? (Just a code name)
    println("What are the top highly populated departments in France ? (Just a code name)")
    people.groupBy($"Departement").agg(sum($"Population")).sort(sum($"Population").desc).show()

    val dep = spark.read.text("data/input/departements.txt")

    val dep2 = dep.map(f=>{
      val elements = f.getString(0).split(",")
      (elements(0),elements(1))
    })

    val resultDf = people.join(broadcast(dep2),people("departement") <=> dep2("_2"))

    //What are the top highly populated departments in France ? (Use join to display the name)
    println("What are the top highly populated departments in France ? (Use join to display the name)")
    resultDf.groupBy($"_1",$"Departement").agg(sum($"Population")).sort(sum($"Population").desc).show()
  }

  def exec3(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.printSchema()
    toursDF.show()

    //1. How many unique levels of difficulties ?
    println("1. How many unique levels of difficulties ?")
    toursDF.groupBy($"tourDifficulty").count().show()

    //2. What is the min/max/avg of tour prices ?
    println("2. What is the min/max/avg of tour prices ?")
    toursDF.agg(min($"tourPrice"),max($"tourPrice"),avg($"tourPrice")).show()

    //3. What is the min/max/avg of price for each level of difficultiy ?
    println("3. What is the min/max/avg of price for each level of difficultiy ?")
    toursDF.groupBy($"tourDifficulty").agg(min($"tourPrice"),max($"tourPrice"),avg($"tourPrice")).show()

    //4. What is the min/max/avg of price and min/max/avg of duration (length) for each level of difficulty ?
    println("4. What is the min/max/avg of price and min/max/avg of duration (length) for each level of difficulty ?")
    toursDF.groupBy($"tourDifficulty").agg(min($"tourPrice"),max($"tourPrice"),avg($"tourPrice"),min($"tourLength"),max($"tourLength"),avg($"tourLength")).show()

    //5. Display the top 10 "tourTags" (use explode)
    println("Display the top 10 \"tourTags\" (use explode)")
    toursDF.select($"tourName",explode($"tourTags")).groupBy($"col").count().sort(desc("count")).limit(10).show()

    //6. Relationship between top 10 "tourTags" and "tourDifficulty"
    println("6. Relationship between top 10 \"tourTags\" and \"tourDifficulty\"")
    toursDF.select($"tourDifficulty",explode($"tourTags")).groupBy($"col",$"tourDifficulty").count().sort(desc("count")).limit(10).show()

    //7. What is the min/max/avg of price in "tourTags" and "tourDifficulty" relationship ? (sort by average)
    println("7. What is the min/max/avg of price in \"tourTags\" and \"tourDifficulty\" relationship ? (sort by average)")
    toursDF.select($"tourDifficulty",explode($"tourTags"),$"tourPrice").groupBy($"col",$"tourDifficulty").agg(min($"tourPrice"),max($"tourPrice"),avg($"tourPrice")).sort(avg($"tourPrice").desc).show()
  }
}
