package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.executor.JsonGraphQlExecutor
import uk.co.gresearch.spark.dgraph.connector.{Json, Partition, UidRange}

class TestUidCardinalityEstimator extends FunSpec {

  def doTestUidCardinalityEstimatorBase(estimator: UidCardinalityEstimatorBase,
                                        expectedEstimationWithoutRange: Option[Long]): Unit = {

    it("should estimate partition's uid range") {
      val partition = Partition(Seq.empty, None, Some(UidRange(0, 1000)), None)
      val actual = estimator.uidCardinality(partition)
      assert(actual.isDefined)
      assert(actual.get === 1000)
    }

    it("should estimate partition without uid range") {
      val partition = Partition(Seq.empty, None, None, None)
      val actual = estimator.uidCardinality(partition)
      assert(actual === expectedEstimationWithoutRange)
    }

  }

  describe("UidCardinalityEstimatorBase") {
    doTestUidCardinalityEstimatorBase(new UidCardinalityEstimatorBase {}, None)
  }

  describe("MaxLeaseIdUidCardinalityEstimator") {
    val estimator = MaxLeaseIdUidCardinalityEstimator(1234)
    doTestUidCardinalityEstimatorBase(estimator, Some(1234))

    it("should fail on negative or zero max uids") {
      assertThrows[IllegalArgumentException] {
        UidCardinalityEstimator.forMaxLeaseId(-1)
      }
      assertThrows[IllegalArgumentException] {
        UidCardinalityEstimator.forMaxLeaseId(0)
      }
    }

  }

  describe("QueryUidCardinalityEstimator") {
    val executor = new JsonGraphQlExecutor {
      override def query(query: connector.GraphQl): connector.Json =
        Json(
          """{
            |  "result": [
            |    {
            |      "count": 1234
            |    }
            |  ]
            |}
            |""".stripMargin)
    }
    val estimator = new QueryUidCardinalityEstimator(executor)

    doTestUidCardinalityEstimatorBase(estimator, Some(1234))

    it("should return no estimation and warn for multiple cardinality results") {
      val executor = new JsonGraphQlExecutor {
        override def query(query: connector.GraphQl): connector.Json =
          Json(
            """{
              |  "result": [
              |    {
              |      "count": 10
              |    },
              |    {
              |      "count": 20
              |    }
              |  ]
              |}
              |""".stripMargin)
      }
      val estimator = new QueryUidCardinalityEstimator(executor)
      val partition = Partition(Seq.empty, None, None, None)
      val actual = estimator.uidCardinality(partition)
      assert(actual.isEmpty)
    }
  }

}
