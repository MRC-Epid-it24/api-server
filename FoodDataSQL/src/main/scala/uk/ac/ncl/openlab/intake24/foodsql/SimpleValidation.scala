package uk.ac.ncl.openlab.intake24.foodsql

import java.sql.Connection

import anorm.{Macro, SQL, SqlParser}
import uk.ac.ncl.openlab.intake24.errors._

trait SimpleValidation {

  private val foodAndLocaleValidationQuery = "SELECT (SELECT true FROM foods WHERE code={food_code}) AS food_exists, (SELECT true FROM locales WHERE id={locale_id}) as locale_exists"

  private val categoryAndLocaleValidationQuery = "SELECT (SELECT true FROM categories WHERE code={category_code}) AS category_exists, (SELECT true FROM locales WHERE id={locale_id}) as locale_exists"

  private val localeValidationQuery = "SELECT (SELECT true FROM locales WHERE id={locale_id}) AS locale_exists"

  private case class FoodAndLocaleValidationRow(food_exists: Option[Boolean], locale_exists: Option[Boolean])

  private case class CategoryAndLocaleValidationRow(category_exists: Option[Boolean], locale_exists: Option[Boolean])

  private def checkTransactionIsolation[E >: UnexpectedDatabaseError](conn: Connection)(block: => Either[E, Unit]) = {
    if (conn.getAutoCommit() || (conn.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ))
      Left(UnexpectedDatabaseError(new RuntimeException("Connection must be in manual commit, repeatable read mode for simple validation")))
    else
      block
  }

  protected def validateLocale(locale: String)(implicit conn: java.sql.Connection): Either[LocaleError, Unit] = {
    checkTransactionIsolation[LocaleError](conn) {
      val validation = SQL(localeValidationQuery).on("locale_id" -> locale).executeQuery().as(SqlParser.bool("locale_exists").?.single)

      if (validation.isEmpty)
        Left(UndefinedLocale(new RuntimeException()))
      else
        Right(())
    }
  }

  protected def validateFoodAndLocale(foodCode: String, locale: String)(implicit conn: java.sql.Connection): Either[LocalLookupError, Unit] = {
    checkTransactionIsolation[LocalLookupError](conn) {
      val validation = SQL(foodAndLocaleValidationQuery).on("food_code" -> foodCode, "locale_id" -> locale).executeQuery().as(Macro.namedParser[FoodAndLocaleValidationRow].single)

      if (validation.food_exists.isEmpty) {
        Left(RecordNotFound(new RuntimeException(foodCode)))
      } else if (validation.locale_exists.isEmpty) {
        Left(UndefinedLocale(new RuntimeException(locale)))
      } else {
        Right(())
      }
    }
  }

  protected def validateCategoryAndLocale(categoryCode: String, locale: String)(implicit conn: Connection): Either[LocalLookupError, Unit] = {
    checkTransactionIsolation[LocalLookupError](conn) {
      val validation = SQL(categoryAndLocaleValidationQuery).on("category_code" -> categoryCode, "locale_id" -> locale).executeQuery().as(Macro.namedParser[CategoryAndLocaleValidationRow].single)

      if (validation.category_exists.isEmpty) {
        Left(RecordNotFound(new RuntimeException(categoryCode)))
      } else if (validation.locale_exists.isEmpty) {
        Left(UndefinedLocale(new RuntimeException(locale)))
      } else {
        Right(())
      }
    }
  }
}
