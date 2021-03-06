package uk.ac.ncl.openlab.intake24.foodsql.admin

import javax.sql.DataSource

import anorm.{NamedParameter, SQL, sqlToSimple}
import com.google.inject.Inject
import com.google.inject.name.Named
import org.slf4j.LoggerFactory
import uk.ac.ncl.openlab.intake24.api.shared.admin.SplitList
import uk.ac.ncl.openlab.intake24.errors.UnexpectedDatabaseError
import uk.ac.ncl.openlab.intake24.foodsql.foodindex.FoodIndexDataSharedImpl
import uk.ac.ncl.openlab.intake24.services.fooddb.admin.FoodIndexDataAdminService
import uk.ac.ncl.openlab.intake24.sql.SqlDataService

class FoodIndexDataAdminStandaloneImpl @Inject()(@Named("intake24_foods") val dataSource: DataSource) extends FoodIndexDataAdminImpl

trait FoodIndexDataAdminImpl extends FoodIndexDataAdminService with SqlDataService with FoodIndexDataSharedImpl {

  private val logger = LoggerFactory.getLogger(classOf[FoodIndexDataAdminImpl])

  def deleteSynsets(locale: String): Either[UnexpectedDatabaseError, Unit] = tryWithConnection {
    implicit conn =>
      logger.debug("Deleting existing synonym sets")
      SQL("DELETE FROM synonym_sets WHERE locale_id={locale_id}").on('locale_id -> locale).execute()
      Right(())
  }

  def deleteSplitList(locale: String): Either[UnexpectedDatabaseError, Unit] = tryWithConnection {
    implicit conn =>

      logger.debug("Deleting existing split list")
      conn.setAutoCommit(false)

      SQL("DELETE FROM split_words WHERE locale_id={locale_id}").on('locale_id -> locale).execute()
      SQL("DELETE FROM split_list WHERE locale_id={locale_id}").on('locale_id -> locale).execute()
      conn.commit()
      Right(())
  }

  def createSynsets(synsets: Seq[Set[String]], locale: String): Either[UnexpectedDatabaseError, Unit] = tryWithConnection {
    implicit conn =>

      conn.setAutoCommit(false)

      if (!synsets.isEmpty) {
        logger.debug("Writing synonym sets to database")

        val synonymSetsParams = synsets.map {
          set =>
            Seq[NamedParameter]('locale -> locale, 'synonyms -> set.mkString(" "))
        }

        batchSql("""INSERT INTO synonym_sets VALUES (DEFAULT, {locale}, {synonyms})""", synonymSetsParams).execute()
      } else
        logger.debug("Synonym sets file is empty")

      conn.commit()

      Right(())
  }

  def createSplitList(splitList: SplitList, locale: String): Either[UnexpectedDatabaseError, Unit] = tryWithConnection {
    implicit conn =>
      conn.setAutoCommit(false)

      if (!splitList.splitWords.isEmpty) {
        logger.debug("Writing split list to database")

        SQL("""INSERT INTO split_words VALUES (DEFAULT, {locale}, {words})""")
          .on('locale -> "en_GB", 'words -> splitList.splitWords.mkString(" "))
          .executeInsert()

        val splitListParams = splitList.keepPairs.map {
          case (firstWord, words) =>
            Seq[NamedParameter]('locale -> locale, 'first_word -> firstWord, 'words -> words.mkString(" "))
        }.toSeq

        if (!splitListParams.isEmpty)
          batchSql("""INSERT INTO split_list VALUES (DEFAULT, {locale}, {first_word}, {words})""", splitListParams).execute()
        else
          logger.debug("Split list parameter list is empty")
      } else
        logger.debug("Split list file is empty")

      conn.commit()
      Right(())
  }
}
