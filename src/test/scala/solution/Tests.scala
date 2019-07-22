package solution

import org.apache.spark.sql._
import org.joda.time.DateTime
import org.scalatest._

import solution.Solution._

class SolutionSpec extends FlatSpec {

  val txns = readCSV("txns.csv")
  val catalog = readCSV("catalog.csv")
  val testdate = new DateTime(2018, 3, 1, 0, 0)
  var df: DataFrame = null
  val cust = "d378e25f-1127-4812-ae1b-2881b7fbee0e"

  def v(col: String): Double = {
    df.filter(df("customer_uuid") === cust)
      .select(col)
      .collect
      .map(_.getDouble(0))
      .head
  }

  it should "compute a solution" in {
    df = solve(testdate, txns = txns, catalog = catalog).cache
  }

  it should "have a row for every customer" in {
    assert(df.count == 10)
  }

  it should "compute weekly_visits correctly" in {
    assert(v("weekly_visits") == 1.625)
  }

  it should "compute weekly_nonfood_purchases correctly" in {
    assert(v("weekly_nonfood_purchases") == 1.375)
  }

  it should "compute weekly_food_purchases correctly" in {
    assert(v("weekly_food_purchases") == 0.25)
  }

  it should "compute weekly_spend correctly" in {
    assert(v("weekly_spend") == 5.7625)
  }
}
