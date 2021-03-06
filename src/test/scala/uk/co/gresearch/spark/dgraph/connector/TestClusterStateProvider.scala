/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.DgraphTestCluster

class TestClusterStateProvider extends FunSpec with DgraphTestCluster {

  describe("ClusterStateProvider") {

    it("should retrieve cluster state") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Target(cluster.grpc))
      assert(state.isDefined)
      assert(state.get === ClusterState(
        Map("1" -> Set(Target(cluster.grpc))),
        Map("1" -> Set("name", "dgraph.graphql.schema", "starring", "dgraph.graphql.xid", "running_time", "release_date", "director", "revenue", "dgraph.type")),
        10000,
        state.get.cid
      ))
    }

    it("should retrieve cluster states") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Seq(Target(cluster.grpc), Target(cluster.grpcLocalIp)))
      assert(state === ClusterState(
        Map("1" -> Set(Target(cluster.grpc))),
        Map("1" -> Set("name", "dgraph.graphql.schema", "starring", "dgraph.graphql.xid", "running_time", "release_date", "director", "revenue", "dgraph.type")),
        10000,
        state.cid
      ))
    }

    it("should fail for unavailable cluster") {
      val provider = new ClusterStateProvider {}
      assertThrows[RuntimeException] {
        provider.getClusterState(Seq(Target("localhost:1001")))
      }
    }

    it("should return None for unavailable cluster") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Target("localhost:1001"))
      assert(state.isEmpty)
    }

  }

}
