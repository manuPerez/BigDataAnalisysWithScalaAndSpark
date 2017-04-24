package timeusage

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, _}

import scala.collection.immutable.Seq

/** Main class */
object TimeUsage {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()

    val df = timeUsageGroupedSql(finalDf)
    val summerDs = timeUsageSummaryTyped(df)
    val finalDs = timeUsageGroupedTyped(summerDs)
    finalDs.show()
  }

  /** @return The read DataFrame along with its column names. */
  def read(resource: String): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data: RDD[Row] =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    (headerColumns, dataFrame)
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNames Column names of the DataFrame
    */
  def dfSchema(columnNames: List[String]): StructType = {
    val fields: List[StructField] = columnNames.tail
      .map(field => StructField(field, DoubleType, nullable = false))
    StructType(StructField(columnNames.head, StringType, nullable = false) :: fields)
  }

  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = {
    var seqString: Seq[Any] = Seq(line.head)

    val seqDoubles: Seq[Double] = line.tail.map(a => a.toDouble).toSeq
    val seqFinal: Seq[Any] = seqString ++ seqDoubles

    val myRow: Row = Row.fromSeq(seqFinal)
    myRow
  }

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *    “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *    “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    val columnsPrimaryNeeds: List[Column] = columnNames
      .filter(field => field.startsWith("t01")
                    || field.startsWith("t03")
                    || field.startsWith("t11")
                    || field.startsWith("t1801")
                    || field.startsWith("t1803"))
      .map(field => col(field))

    val columnsWorking: List[Column] = columnNames
      .filter(field => field.startsWith("t05")
                    || field.startsWith("t1805"))
      .map(field => col(field))

    val subColumnsOtherActivities: List[Column] = columnNames
      .filter(field => field.startsWith("t02")
                    || field.startsWith("t04")
                    || field.startsWith("t06")
                    || field.startsWith("t07")
                    || field.startsWith("t08")
                    || field.startsWith("t09")
                    || field.startsWith("t10")
                    || field.startsWith("t12")
                    || field.startsWith("t13")
                    || field.startsWith("t14")
                    || field.startsWith("t15")
                    || field.startsWith("t16")
                    || field.startsWith("t18"))
      .map(field => col(field))

    val columnsOtherActivities = subColumnsOtherActivities
      .filterNot(columnsPrimaryNeeds.contains(_))
      .filterNot(columnsWorking.contains(_))

    (columnsPrimaryNeeds, columnsWorking, columnsOtherActivities)
  }

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame should have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
    *
    * Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame = {
    val workingStatusProjection: Column =
      when(df("telfs") >= 1 && df("telfs") < 3, "working")
        .otherwise("not working")
        .as("working")

    val sexProjection =
      when(df("tesex") === 1, "male")
      .otherwise("female")
      .as("sex")

    val ageProjection: Column =
      when(df("teage") >= 15 && df("teage") <= 22, "young")
        .otherwise(
          when(df("teage") >= 23 && df("teage") <= 55, "active")
              .otherwise("elder")
        )
        .as("age")

    val primaryNeedsProjection = primaryNeedsColumns.reduce(_ + _)./(60).alias("primaryNeeds")

    val workProjection = workColumns.reduce(_ + _)./(60).alias("work")

    val otherProjection = otherColumns.reduce(_ + _)./(60).alias("other")

    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force
}

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame should have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *   status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *   and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *   age, rounded with a scale of 1 (using the `round` function).
    *
    * Finally, the resulting DataFrame should be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame = {
    summed.select($"working",$"sex",$"age",$"primaryNeeds",$"work",$"other")
          .groupBy($"working",$"sex",$"age")
//          .groupBy($"age",$"sex",$"working")
          .agg(round(avg($"primaryNeeds"),1).alias("primaryNeeds"),
               round(avg($"work"),1).alias("work"),
               round(avg($"other"),1).alias("other"))
          .orderBy($"working",$"sex",$"age")
  }

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName: String = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    "SELECT working, sex, age, " +
      "round(avg(primaryNeeds),1) as primaryNeeds, " +
      "round(avg(work),1) as work, " +
      "round(avg(other),1) as other " +
      "FROM summed " +
      "GROUP BY working, sex, age " +
//      "GROUP BY age, sex, working " +
      "ORDER BY working, sex, age"

  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    *
    * Hint: you should use the `getAs` method of `Row` to look up columns and
    * cast them at the same time.
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
      timeUsageSummaryDf.map(
        row => TimeUsageRow(row.getAs("working"),
                            row.getAs("sex"),
                            row.getAs("age"),
                            row.getAs("primaryNeeds"),
                            row.getAs("work"),
                            row.getAs("other")))


  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    *
    * Hint: you should use the `groupByKey` and `typed.avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed
    val ds: Dataset[((String, String, String), Double, Double, Double)] =
      summed
            .groupByKey(t => (t.working, t.sex, t.age))
//            .groupByKey(t => (t.age, t.sex, t.working))
            .agg(round(typed.avg[TimeUsageRow](_.primaryNeeds), 1).as[Double].name("primaryNeeds"),
                 round(typed.avg[TimeUsageRow](_.work),1).as[Double].name("work"),
                 round(typed.avg[TimeUsageRow](_.other),1).as[Double].name("other"))

    ds.map(v1 => TimeUsageRow(v1._1._1, v1._1._2, v1._1._3, v1._2, v1._3, v1._4)).orderBy($"working", $"sex", $"age")
  }
}

/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)