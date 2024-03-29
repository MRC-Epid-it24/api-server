package uk.ac.ncl.openlab.intake24.foodsql.shared

import anorm.{Macro, SQL}
import uk.ac.ncl.openlab.intake24.api.data.{PortionSizeMethod, PortionSizeMethodParameter}
import uk.ac.ncl.openlab.intake24.errors.LocalLookupError
import uk.ac.ncl.openlab.intake24.foodsql.{FirstRowValidation, FirstRowValidationClause}
import uk.ac.ncl.openlab.intake24.sql.SqlResourceLoader

trait FoodPortionSizeShared extends SqlResourceLoader with FirstRowValidation {

  protected case class PsmResultRow(id: Long, method: String, description: String, image_url: String, use_for_recipes: Boolean, conversion_factor: Double, param_name: Option[String], param_value: Option[String])

  protected val psmResultRowParser = Macro.namedParser[PsmResultRow]

  private lazy val idParamNames = Set("serving-image-set", "leftovers-image-set", "guide-image-id", "drinkware-id")

  protected def mkPortionSizeMethods(rows: Seq[PsmResultRow]) =
  // FIXME: surely there is a better method to group records preserving order...
    rows.groupBy(_.id).toSeq.sortBy(_._1).map {
      case (id, rows) =>
        val params = rows.filterNot(r => r.param_name.isEmpty || r.param_value.isEmpty).map(row => PortionSizeMethodParameter(row.param_name.get, row.param_value.get))
        val head = rows.head
        val imageUrl = rows.find(row => row.param_name.map(idParamNames.contains(_)).getOrElse(false)).getOrElse(head).image_url

        PortionSizeMethod(head.method, head.description, imageUrl, head.use_for_recipes, head.conversion_factor, params)
    }

  // FIXME: The query is too complex for what it does
  private lazy val foodPsmQuery = sqlFromResource("shared/food_portion_size_methods.sql")

  protected def getFoodPortionSizeMethodsQuery(code: String, locale: String)(implicit conn: java.sql.Connection): Either[LocalLookupError, Seq[PortionSizeMethod]] = {
    val psmResults = SQL(foodPsmQuery).on('food_code -> code, 'locale_id -> locale).executeQuery()

    parseWithLocaleAndFoodValidation(code, psmResults, psmResultRowParser.+)(Seq(FirstRowValidationClause("id", () => Right(List())))).right.map(mkPortionSizeMethods)
  }
}
