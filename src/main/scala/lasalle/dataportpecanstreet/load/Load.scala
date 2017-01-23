package lasalle.dataportpecanstreet.load

import lasalle.dataportpecanstreet.extract.table.TableMetadata
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.{MultiBulkWriteResult, WriteResult}
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

object Load {

  def load(tableMetadata: TableMetadata, tableData: BSONDocument): Future[WriteResult] = {
    val collection: BSONCollection = MongoEnvironment.mainDb.collection(tableMetadata.table)
    collection.insert(tableData)
  }

  def load(tableMetadata: TableMetadata, tableData: Iterable[BSONDocument]): Future[MultiBulkWriteResult] = {
    val collection: BSONCollection = MongoEnvironment.mainDb.collection(tableMetadata.table)
    val bulkDocs = tableData.toSeq.map(implicitly[collection.ImplicitlyDocumentProducer](_))
    collection.bulkInsert(ordered = false)(bulkDocs: _*)
  }

}
