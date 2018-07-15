package lasalle.dataportpecanstreet

import java.sql.ResultSet

import lasalle.dataportpecanstreet.extract.Extract.iterateOverResultSet

import scala.annotation.tailrec

object EGauge {

  @tailrec
  def iterateOverResultSet[R](resultSet: ResultSet, accum: R, f: (ResultSet, R) => R): R = {
    if (resultSet.next()) {
      iterateOverResultSet(resultSet, f(resultSet, accum), f)
    } else accum
  }

  def main(args: Array[String]): Unit = {
    lasalle.dataportpecanstreet.Connection.connect().map { connection =>

      val query = s"""select dataid from ${Config.PostgreSqlServer.schema}.metadata where date_enrolled > '01-01-2013' and date_withdrawn > '02-01-2013' and (winecooler1 = 'yes' or pool1 = 'yes' or lights_plugs4 = 'yes' or icemaker1 = 'yes' or bedroom1 = 'yes' or bathroom1 = 'yes' or freezer1 = 'yes' or furnace1 = 'yes' or livingroom1 = 'yes' or air1 = 'yes' or security1 = 'yes' or refrigerator1 = 'yes' or lights_plugs3 = 'yes' or oven2 = 'yes' or lights_plugs5 = 'yes' or garage1 = 'yes' or range1 = 'yes' or bedroom2 = 'yes' or waterheater2 = 'yes' or bathroom2 = 'yes' or air3 = 'yes' or kitchen1 = 'yes' or disposal1 = 'yes' or office1 = 'yes' or car1 = 'yes' or venthood1 = 'yes' or diningroom2 = 'yes' or lights_plugs2 = 'yes' or bedroom5 = 'yes' or aquarium1 = 'yes' or outsidelights_plugs1 = 'yes' or outsidelights_plugs2 = 'yes' or poolpump1 = 'yes' or grid = 'yes' or oven1 = 'yes' or clotheswasher1 = 'yes' or waterheater1 = 'yes' or kitchen2 = 'yes' or lights_plugs6 = 'yes' or bedroom3 = 'yes' or dryg1 = 'yes' or drye1 = 'yes' or refrigerator2 = 'yes' or kitchenapp1 = 'yes' or pool2 = 'yes' or lights_plugs1 = 'yes' or utilityroom1 = 'yes' or clotheswasher_dryg1 = 'yes' or dishwasher1 = 'yes' or heater1 = 'yes' or diningroom1 = 'yes' or airwindowunit1 = 'yes' or poollight1 = 'yes' or furnace2 = 'yes' or livingroom2 = 'yes' or microwave1 = 'yes' or sprinkler1 = 'yes' or kitchenapp2 = 'yes' or housefan1 = 'yes' or jacuzzi1 = 'yes' or bedroom4 = 'yes' or shed1 = 'yes' or air2 = 'yes' or garage2 = 'yes' or pump1 = 'yes')"""

      val statement = connection.createStatement()

      val resultSet = statement.executeQuery(query)

      val dataids = iterateOverResultSet(resultSet, List.empty[Int], (rs, accum: List[Int]) => {
        rs.getInt("dataid") +: accum
      })

      val queryDataids = dataids.mkString("""dataid = """", """" or dataid = """", """"""")


      print("a")

    }
  }

}
