package lasalle.dataportpecanstreet.load


import lasalle.dataportpecanstreet.Config
import lasalle.dataportpecanstreet.extract.table.{TableData, TableMetadata}
import lasalle.dataportpecanstreet.transform.Transform
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands._

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

object Load {

  def loadMetadata(tableMetadata: TableMetadata, toTableName: String): Future[WriteResult] = {
    Config.execute[Future[WriteResult]](Config.Etl.Load.metadata)(Future(DefaultWriteResult(ok = true, 0, Seq.empty, None, None, None))) {

      val collection: BSONCollection = MongoEnvironment.mainDb.collection(toTableName + "_metadata")
      collection.insert(Transform.columnsMetadataToBson(tableMetadata.metadata))

    }
  }

  def load(tableMetadata: TableMetadata, row: TableData.Row, toTableName: String): Future[WriteResult] = {

    val collection: BSONCollection = MongoEnvironment.mainDb.collection(toTableName)
    collection.insert(
      Transform.rowToBsonDocument(row)(tableMetadata)
    )
  }

  def load(tableMetadata: TableMetadata, tableData: TableData.Rows, toTableName: String): Future[MultiBulkWriteResult] = {

    val collection: BSONCollection = MongoEnvironment.mainDb.collection(toTableName)

    val bulkDocs = Transform.rowsToBsonDocument(tableData)(tableMetadata)
      .map(implicitly[collection.ImplicitlyDocumentProducer](_))

    collection.bulkInsert(ordered = false)(bulkDocs: _*)
  }

}
