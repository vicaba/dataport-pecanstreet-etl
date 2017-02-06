package lasalle.dataportpecanstreet.load

import java.util.concurrent.Executors

import lasalle.dataportpecanstreet.extract.table.TableMetadata
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.{MultiBulkWriteResult, WriteResult}
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object Load {

  val BulkInsertLimit = 1000

  val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  def load(tableMetadata: TableMetadata, tableData: BSONDocument): Future[WriteResult] = {
    val collection: BSONCollection = MongoEnvironment.mainDb.collection(tableMetadata.table)
    collection.insert(tableData)
  }

  def load(tableMetadata: TableMetadata, tableData: Iterable[BSONDocument]): Future[Iterable[WriteResult]] = {

    val collection: BSONCollection = MongoEnvironment.mainDb.collection(tableMetadata.table)

    val it = tableData.map( d => this.load(tableMetadata, d))
    Future.sequence(it)
  }
}
