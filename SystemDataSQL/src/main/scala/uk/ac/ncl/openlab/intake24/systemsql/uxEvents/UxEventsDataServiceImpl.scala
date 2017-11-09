package uk.ac.ncl.openlab.intake24.systemsql.uxEvents

import java.time.ZonedDateTime
import javax.inject.{Inject, Named}
import javax.sql.DataSource

import anorm.{Macro, SQL}
import uk.ac.ncl.openlab.intake24.errors.{CreateError, FailedValidation, UnexpectedDatabaseError}
import uk.ac.ncl.openlab.intake24.services.systemdb.uxEvents.{UxEventIn, UxEventOut, UxEventsDataService}
import uk.ac.ncl.openlab.intake24.sql.SqlDataService

/**
  * Created by Tim Osadchiy on 08/11/2017.
  */
class UxEventsDataServiceImpl @Inject()(@Named("intake24_system") val dataSource: DataSource) extends UxEventsDataService with SqlDataService {

  private case class UxEventRow(id: Long, event_categories: Array[String], event_type: String, data: String, created: ZonedDateTime) {
    def toEvent = UxEventOut(id, event_categories, event_type, data, created)
  }

  private val tableName = "ux_events"
  private val returningFields = "id, event_categories::varchar[], event_type, data::TEXT, created"
  private val insertQ =
    s"""
       |INSERT INTO $tableName (event_categories, event_type, data)
       |VALUES ({event_categories}::varchar[], {event_type}, {data}::JSON)
       |RETURNING $returningFields;
    """.stripMargin
  private val selectQ = s"SELECT $returningFields FROM $tableName;"

  override def create(uxEvent: UxEventIn): Either[CreateError, UxEventOut] =
    if (uxEvent.eventCategories.isEmpty) {
      Left(FailedValidation(new Exception("UxEvent must have at least one category")))
    } else tryWithConnection {
      implicit conn =>
        val cats = s"{${uxEvent.eventCategories.mkString(",")}}"
        val r = SQL(insertQ).on('event_categories -> cats,
          'event_type -> uxEvent.eventType,
          'data -> uxEvent.data)
          .executeQuery().as(Macro.namedParser[UxEventRow].single)
        Right(r.toEvent)
    }

}
