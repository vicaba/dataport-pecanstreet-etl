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

      val appliances = "winecooler1,pool1,lights_plugs4,icemaker1,bedroom1,bathroom1,freezer1,furnace1,livingroom1,air1,security1,refrigerator1,lights_plugs3,oven2,lights_plugs5,garage1,range1,bedroom2,waterheater2,bathroom2,air3,kitchen1,disposal1,office1,car1,venthood1,diningroom2,lights_plugs2,bedroom5,aquarium1,outsidelights_plugs1,outsidelights_plugs2,poolpump1,grid,oven1,clotheswasher1,waterheater1,kitchen2,lights_plugs6,bedroom3,dryg1,drye1,refrigerator2,kitchenapp1,pool2,lights_plugs1,utilityroom1,clotheswasher_dryg1,dishwasher1,heater1,use,diningroom1,airwindowunit1,poollight1,furnace2,livingroom2,microwave1,sprinkler1,kitchenapp2,housefan1,jacuzzi1,bedroom4,shed1,air2,garage2,gen,pump1".split(",")

      val egaugeAppliancesQueryPart = appliances.mkString("", " = 'yes' or ", " = 'yes'")

      val enrolledDataidsQuery = s"""select dataid from ${Config.PostgreSqlServer.schema}.metadata where date_enrolled < '2015-01-01' and date_withdrawn > '2015-01-02' and ($egaugeAppliancesQueryPart)"""

      val enrolledDataidsResultSet = connection.createStatement().executeQuery(enrolledDataidsQuery)

      val dataids = iterateOverResultSet(enrolledDataidsResultSet, List.empty[Int], (rs, accum: List[Int]) => {
        rs.getInt("dataid") +: accum
      })

      val selectDataidsQueryPart = dataids.mkString("dataid = ", " or dataid = ", "")

      val egaugeDataQuery = s"""select dataid, ${appliances.mkString(", ")} , localhour from  ${Config.PostgreSqlServer.schema}.electricity_egauge_hours where localhour between '2015-01-01' and '2015-01-01 23:59:00' and ($selectDataidsQueryPart) order by dataid, localhour"""


      val egaugeData = dataids.map { dataId =>

        val _egaugeDataQuery = s"""select dataid, ${appliances.mkString(", ")} , localhour from  ${Config.PostgreSqlServer.schema}.electricity_egauge_hours where localhour between '2015-01-01' and '2015-01-01 23:59:00' and dataid = $dataId order by localhour"""


        val rs = connection.createStatement().executeQuery(_egaugeDataQuery)

        val appliancesPerDataId = iterateOverResultSet(rs, List.empty[Map[String, BigDecimal]], (_rs, accum: List[Map[String, BigDecimal]]) => {
          assert(dataId == _rs.getInt("dataid"))
          val appliancesAtTime = appliances.map { appliance =>
            appliance -> Option(_rs.getBigDecimal(appliance)).fold(BigDecimal(0))(v => v)
          }.toMap
          accum :+ appliancesAtTime
        })

        val appliancesPerDataIdTransposed = appliances.map { appliance =>
          appliance -> appliancesPerDataId.map { m =>
            m.getOrElse(appliance, BigDecimal(0))
          }
        }

        dataId -> appliancesPerDataIdTransposed

      }.toMap

      print("a")

    }
  }

}
