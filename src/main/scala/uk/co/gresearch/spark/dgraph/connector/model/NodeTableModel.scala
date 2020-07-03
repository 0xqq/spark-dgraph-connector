package uk.co.gresearch.spark.dgraph.connector.model
import uk.co.gresearch.spark.dgraph.connector.encoder.InternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.{Chunk, GraphQl, PartitionQuery}

/**
 * Models only the nodes of a graph as a table.
 */
case class NodeTableModel(execution: ExecutorProvider, encoder: InternalRowEncoder, chunkSize: Option[Int]) extends GraphTableModel {

  override def withEncoder(encoder: InternalRowEncoder): GraphTableModel =
    copy(encoder = encoder)

  /**
   * Turn a partition query into a GraphQl query.
   *
   * @param query partition query
   * @param chunk chunk of the result set to query
   * @return graphql query
   */
  override def toGraphQl(query: PartitionQuery, chunk: Option[Chunk]): GraphQl =
    query.forProperties(chunk)

}
