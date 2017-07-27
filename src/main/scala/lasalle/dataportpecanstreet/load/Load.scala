package lasalle.dataportpecanstreet.load

import java.util.concurrent.Executors

import lasalle.dataportpecanstreet.extract.table.TableMetadata
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.{MultiBulkWriteResult, WriteResult}
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object Load {

  val BulkInsertLimit = 1000

  val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  def load(tableMetadata: TableMetadata, tableData: BSONDocument): Future[WriteResult] = {
    val collection: BSONCollection = MongoEnvironment.mainDb.collection(tableMetadata.table)
    collection.insert(tableData)
  }

  def load(tableMetadata: TableMetadata, tableData: Iterable[BSONDocument]): Future[MultiBulkWriteResult] = {

    val collection: BSONCollection = MongoEnvironment.mainDb.collection(tableMetadata.table)

    val bulkDocs = tableData.map(implicitly[collection.ImplicitlyDocumentProducer](_)).toSeq

    collection.bulkInsert(ordered = false)(bulkDocs: _*)

    //val it = tableData.map( d => this.load(tableMetadata, d))
    //Future.sequence(it)
  }
}
