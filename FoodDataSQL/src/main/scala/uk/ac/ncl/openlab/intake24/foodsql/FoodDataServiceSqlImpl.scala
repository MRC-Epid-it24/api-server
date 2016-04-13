/*
This file is part of Intake24.

Copyright 2015, 2016 Newcastle University.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package uk.ac.ncl.openlab.intake24.foodsql

import java.sql.Connection
import java.util.UUID
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import anorm.Macro
import anorm.NamedParameter.symbol
import anorm.SQL
import anorm.SqlParser.str
import anorm.sqlToSimple
import javax.sql.DataSource
import net.scran24.fooddef.AsServedImage
import net.scran24.fooddef.AsServedSet
import net.scran24.fooddef.CategoryContents
import net.scran24.fooddef.CategoryHeader
import net.scran24.fooddef.DrinkScale
import net.scran24.fooddef.DrinkwareSet
import net.scran24.fooddef.Food
import net.scran24.fooddef.FoodData
import net.scran24.fooddef.FoodGroup
import net.scran24.fooddef.FoodHeader
import net.scran24.fooddef.GuideImage
import net.scran24.fooddef.GuideImageWeightRecord
import net.scran24.fooddef.InheritableAttributes
import net.scran24.fooddef.PortionSizeMethod
import net.scran24.fooddef.PortionSizeMethodParameter
import net.scran24.fooddef.Prompt
import net.scran24.fooddef.SplitList
import net.scran24.fooddef.VolumeFunction
import uk.ac.ncl.openlab.intake24.services.FoodDataService
import net.scran24.fooddef.FoodLocal
import net.scran24.fooddef.Category
import net.scran24.fooddef.CategoryLocal
import anorm.SqlParser
import net.scran24.fooddef.GuideHeader
import net.scran24.fooddef.AsServedHeader
import net.scran24.fooddef.DrinkwareHeader
import net.scran24.fooddef.NutrientTable

@Singleton
class FoodDataServiceSqlImpl @Inject() (@Named("intake24_foods") dataSource: DataSource) extends FoodDataService {

  val logger = LoggerFactory.getLogger(classOf[FoodDataServiceSqlImpl])

  def tryWithConnection[T](block: Connection => T) = {
    val conn = dataSource.getConnection()
    try {
      block(conn)
    } catch {
      case e: java.sql.BatchUpdateException => throw new RuntimeException(e.getNextException)
      case e: Throwable => throw new RuntimeException(e)
    } finally {
      conn.close()
    }
  }

  def allFoods(locale: String): Seq[FoodHeader] = tryWithConnection {
    implicit conn =>
      val query =
        """|SELECT code, description, local_description
           |FROM foods JOIN foods_local ON foods.code = foods_local.food_code""".stripMargin

      SQL(query).executeQuery().as(Macro.namedParser[FoodHeaderRow].*).map(_.asFoodHeader)
  }

  def isCategoryCode(code: String): Boolean = tryWithConnection {
    implicit conn =>
      SQL("""SELECT code FROM categories WHERE code={category_code}""").on('category_code -> code).executeQuery().as(SqlParser.str("code").*).nonEmpty
  }

  def isFoodCode(code: String): Boolean = tryWithConnection {
    implicit conn =>
      SQL("""SELECT code FROM foods WHERE code={food_code}""").on('food_code -> code).executeQuery().as(SqlParser.str("code").*).nonEmpty
  }

  def allCategories(locale: String): Seq[CategoryHeader] = tryWithConnection {
    implicit conn =>
      val query =
        """|SELECT code, description, local_description, is_hidden
           |FROM categories JOIN categories_local ON categories.code = categories_local.category_code""".stripMargin

      SQL(query).executeQuery().as(Macro.namedParser[CategoryHeaderRow].*).map(_.asCategoryHeader)
  }

  def uncategorisedFoods(locale: String): Seq[FoodHeader] = tryWithConnection {
    implicit conn =>
      val query =
        """|SELECT code, description, local_description
           |FROM foods JOIN foods_local ON foods.code = foods_local.food_code
           |           LEFT JOIN foods_categories ON foods.code = foods_categories.food_code
           |WHERE category_code IS NULL""".stripMargin

      SQL(query).executeQuery().as(Macro.namedParser[FoodHeaderRow].*).map(_.asFoodHeader)
  }

  case class CategoryHeaderRow(code: String, description: String, local_description: Option[String], is_hidden: Boolean) {
    def asCategoryHeader = CategoryHeader(code, description, local_description, is_hidden)
  }

  def rootCategories(locale: String): Seq[CategoryHeader] = tryWithConnection {
    implicit conn =>
      val query =
        """|SELECT code, description, local_description, is_hidden 
           |FROM categories 
           |  LEFT JOIN categories_categories 
           |    ON categories.code = categories_categories.subcategory_code
           |  LEFT JOIN categories_local
           |    ON categories.code = categories_local.category_code AND categories_local.locale_id = {locale_id} 
           |WHERE categories_categories.category_code IS NULL
           |ORDER BY description""".stripMargin

      SQL(query).on('locale_id -> locale).executeQuery().as(Macro.namedParser[CategoryHeaderRow].*).map(_.asCategoryHeader)
  }

  case class FoodHeaderRow(code: String, description: String, local_description: Option[String]) {
    def asFoodHeader = FoodHeader(code, description, local_description)
  }

  def categoryContents(code: String, locale: String): CategoryContents = tryWithConnection {
    implicit conn =>
      val foodsQuery =
        """|SELECT code, description, local_description 
           |FROM foods_categories 
           |  INNER JOIN foods ON foods.code = foods_categories.food_code 
           |  LEFT JOIN foods_local ON foods.code = foods_local.food_code AND foods_local.locale_id = {locale_id}
           |WHERE category_code = {category_code}
           |ORDER BY local_description""".stripMargin

      val foods = SQL(foodsQuery).on('category_code -> code, 'locale_id -> locale).executeQuery().as(Macro.namedParser[FoodHeaderRow].*).map(_.asFoodHeader)

      val categoriesQuery =
        """|SELECT code, description, local_description, is_hidden
           |FROM categories_categories 
           |     INNER JOIN categories ON categories.code = categories_categories.subcategory_code 
           |     LEFT JOIN categories_local ON categories.code = categories_local.category_code AND categories_local.locale_id = {locale_id}
           |WHERE categories_categories.category_code = {category_code}
           |ORDER BY local_description""".stripMargin

      val categories = SQL(categoriesQuery).on('category_code -> code, 'locale_id -> locale).executeQuery().as(Macro.namedParser[CategoryHeaderRow].*).map(_.asCategoryHeader)

      CategoryContents(foods, categories)
  }

  private case class PsmResultRow(id: Long, method: String, description: String, image_url: String, use_for_recipes: Boolean, param_name: Option[String], param_value: Option[String])

  private case class RecursivePsmResultRow(id: Long, category_code: String, method: String, description: String, image_url: String, use_for_recipes: Boolean, param_name: Option[String], param_value: Option[String])

  private case class RecursiveAttributesRow(same_as_before_option: Option[Boolean], ready_meal_option: Option[Boolean], reasonable_amount: Option[Int])

  private case class FoodRow(code: String, description: String, local_description: Option[String], food_group_id: Long)

  private val psmResultRowParser = Macro.namedParser[PsmResultRow]

  private val recursivePsmResultRowParser = Macro.namedParser[RecursivePsmResultRow]

  private val recursiveAttributesRowParser = Macro.namedParser[RecursiveAttributesRow]

  private val foodRowParser = Macro.namedParser[FoodRow]

  val foodPortionSizeMethodsQuery =
    """|SELECT foods_portion_size_methods.id, method, description, image_url, use_for_recipes,
       |foods_portion_size_method_params.id as param_id, name as param_name, value as param_value
       |FROM foods_portion_size_methods LEFT JOIN foods_portion_size_method_params 
       |  ON foods_portion_size_methods.id = foods_portion_size_method_params.portion_size_method_id
       |WHERE food_code = {food_code} AND locale_id = {locale_id} ORDER BY param_id""".stripMargin

  case class FoodResultRow(version: UUID, code: String, description: String, local_description: Option[String], food_group_id: Long,
    same_as_before_option: Option[Boolean], ready_meal_option: Option[Boolean], reasonable_amount: Option[Int], local_version: Option[UUID])

  val categoryPortionSizeMethodsQuery =
    """|SELECT categories_portion_size_methods.id, method, description, image_url, use_for_recipes,
       |categories_portion_size_method_params.id as param_id, name as param_name, value as param_value
       |FROM categories_portion_size_methods LEFT JOIN categories_portion_size_method_params 
       |  ON categories_portion_size_methods.id = categories_portion_size_method_params.portion_size_method_id
       |WHERE category_code = {category_code} AND locale_id = {locale_id} ORDER BY param_id""".stripMargin

  case class CategoryResultRow(version: UUID, local_version: Option[UUID], code: String, description: String, local_description: Option[String], is_hidden: Boolean, same_as_before_option: Option[Boolean],
    ready_meal_option: Option[Boolean], reasonable_amount: Option[Int])

  private def mkPortionSizeMethods(rows: Seq[PsmResultRow]) =
    // FIXME: surely there is a better method to group records preserving order...
    rows.groupBy(_.id).toSeq.sortBy(_._1).map {
      case (id, rows) =>
        val params = rows.filterNot(r => r.param_name.isEmpty || r.param_value.isEmpty).map(row => PortionSizeMethodParameter(row.param_name.get, row.param_value.get))
        val head = rows.head

        PortionSizeMethod(head.method, head.description, head.image_url, head.use_for_recipes, params)
    }

  private def mkRecursivePortionSizeMethods(rows: Seq[RecursivePsmResultRow]) =
    if (rows.isEmpty)
      Seq()
    else {
      val firstCategoryCode = rows.head.category_code
      mkPortionSizeMethods(rows.takeWhile(_.category_code == firstCategoryCode).map(r => PsmResultRow(r.id, r.method, r.description, r.image_url, r.use_for_recipes, r.param_name, r.param_value)))
    }

  case class NutrientTableRow(nutrient_table_id: String, nutrient_table_code: String)

  def foodDef(code: String, locale: String): Food = tryWithConnection {
    // This is divided into two queries because the portion size estimation method list
    // can be empty, and it's very awkward to handle this case with one big query
    // with a lot of replication
    implicit conn =>

      val psmResults =
        SQL(foodPortionSizeMethodsQuery).on('food_code -> code, 'locale_id -> locale).executeQuery().as(psmResultRowParser.*)

      val portionSizeMethods = mkPortionSizeMethods(psmResults)

      val nutrientTableCodes =
        SQL("""SELECT nutrient_table_id, nutrient_table_code FROM foods_nutrient_tables WHERE food_code = {food_code} AND locale_id = {locale_id}""")
          .on('food_code -> code, 'locale_id -> locale)
          .as(Macro.namedParser[NutrientTableRow].*).map {
            case NutrientTableRow(id, code) => (id -> code)
          }.toMap

      val foodQuery =
        """|SELECT code, description, local_description, food_group_id, same_as_before_option, ready_meal_option,
           |       reasonable_amount, foods.version as version, foods_local.version as local_version 
           |FROM foods 
           |     INNER JOIN foods_attributes ON foods.code = foods_attributes.food_code
           |     LEFT JOIN foods_local ON foods.code = foods_local.food_code AND foods_local.locale_id = {locale_id}
           |WHERE code = {food_code}""".stripMargin

      val foodRowParser = Macro.namedParser[FoodResultRow]

      val result = SQL(foodQuery).on('food_code -> code, 'locale_id -> locale).executeQuery().as(foodRowParser.single)

      Food(result.version, result.code, result.description, result.food_group_id.toInt,
        InheritableAttributes(result.ready_meal_option, result.same_as_before_option, result.reasonable_amount),
        FoodLocal(result.local_version, result.local_description, nutrientTableCodes, portionSizeMethods))

  }

  // Get food data with resolved attribute/portion size method inheritance  
  def foodData(code: String, locale: String): FoodData = tryWithConnection {
    implicit conn =>
      val psmResults =
        SQL(foodPortionSizeMethodsQuery).on('food_code -> code, 'locale_id -> locale).executeQuery().as(psmResultRowParser.*)

      val portionSizeMethods =
        if (!psmResults.isEmpty)
          mkPortionSizeMethods(psmResults)
        else {
          val inheritedPortionSizesQuery =
            """|WITH RECURSIVE t(code, level) AS (
               |(SELECT category_code as code, 0 as level FROM foods_categories WHERE food_code = {food_code} ORDER BY code)
               |  UNION
               |(SELECT category_code as code, level + 1 as level FROM t JOIN categories_categories ON subcategory_code = code ORDER BY code)
               |)
               |SELECT categories_portion_size_methods.id,
               |       category_code, method, description, image_url, use_for_recipes,
               |       categories_portion_size_method_params.id as param_id, name as param_name, 
               |       value as param_value
               |FROM categories_portion_size_methods
               | JOIN t ON code = category_code
               | LEFT JOIN categories_portion_size_method_params 
               |   ON categories_portion_size_methods.id = categories_portion_size_method_params.portion_size_method_id
               |WHERE categories_portion_size_methods.locale_id = {locale_id}              
               |ORDER BY level, id, param_id""".stripMargin

          mkRecursivePortionSizeMethods(SQL(inheritedPortionSizesQuery).on('food_code -> code, 'locale_id -> locale).executeQuery().as(recursivePsmResultRowParser.*))
        }

      val attributesQuery =
        """|WITH RECURSIVE t(code, level) AS (
           |(SELECT category_code as code, 0 as level FROM foods_categories WHERE food_code = {food_code} ORDER BY code)
           |   UNION
           |(SELECT category_code as code, level + 1 as level FROM t JOIN categories_categories ON subcategory_code = code ORDER BY code)
           |)
           |(SELECT same_as_before_option, ready_meal_option, reasonable_amount FROM foods_attributes WHERE food_code = {food_code})
           |UNION ALL
           |(SELECT same_as_before_option, ready_meal_option, reasonable_amount FROM categories_attributes JOIN t ON code = category_code ORDER BY level)
           |UNION ALL
           |(SELECT same_as_before_option, ready_meal_option, reasonable_amount FROM attribute_defaults LIMIT 1)
           """.stripMargin

      val attributeRows = SQL(attributesQuery).on('food_code -> code).executeQuery().as(recursiveAttributesRowParser.+)

      val attributes = attributeRows.tail.foldLeft(attributeRows.head) {
        case (result, row) => {
          RecursiveAttributesRow(
            result.same_as_before_option.orElse(row.same_as_before_option),
            result.ready_meal_option.orElse(row.ready_meal_option),
            result.reasonable_amount.orElse(row.reasonable_amount))
        }
      }

      val foodQuery =
        """|SELECT code, description, local_description, food_group_id
           |FROM foods
           |     LEFT JOIN foods_local ON foods.code = foods_local.food_code AND foods_local.locale_id = {locale_id} 
           |WHERE code = {food_code}""".stripMargin

      val foodRow = SQL(foodQuery).on('food_code -> code, 'locale_id -> locale).executeQuery().as(foodRowParser.single)

      val nutrientTableCodes =
        SQL("""SELECT nutrient_table_id, nutrient_table_code FROM foods_nutrient_tables WHERE food_code = {food_code} AND locale_id = {locale_id}""")
          .on('food_code -> code, 'locale_id -> locale)
          .as(Macro.namedParser[NutrientTableRow].*).map {
            case NutrientTableRow(id, code) => (id -> code)
          }.toMap

      FoodData(foodRow.code, foodRow.description, foodRow.local_description, nutrientTableCodes, foodRow.food_group_id.toInt, portionSizeMethods,
        attributes.ready_meal_option.get, attributes.same_as_before_option.get, attributes.reasonable_amount.get)
  }

  def foodParentCategories(code: String, locale: String): Seq[CategoryHeader] = tryWithConnection {
    implicit conn =>
      SQL("""|SELECT code, description, local_description, is_hidden 
             |FROM foods_categories 
             |     JOIN categories ON foods_categories.category_code = code
             |     LEFT JOIN categories_local ON categories_local.category_code = code AND categories_local.locale_id = {locale_id} 
             |WHERE food_code = {food_code}
             |ORDER BY foods_categories.category_code""".stripMargin)
        .on('food_code -> code, 'locale_id -> locale)
        .executeQuery()
        .as(Macro.namedParser[CategoryHeaderRow].*)
        .map(_.asCategoryHeader)
  }

  def foodAllCategories(code: String, locale: String): Seq[CategoryHeader] = tryWithConnection {
    implicit conn =>
      val query =
        """|WITH RECURSIVE t(code, level) AS (
           |(SELECT category_code as code, 0 as level FROM foods_categories WHERE food_code = {food_code} ORDER BY code)
           | UNION ALL
           |(SELECT category_code as code, level + 1 as level FROM t JOIN categories_categories ON subcategory_code = code ORDER BY code)
           |)
           |SELECT categories.code, description, local_description, is_hidden 
           |FROM t 
           |    JOIN categories on t.code = categories.code
           |    LEFT JOIN categories_local on t.code = categories_local.category_code AND categories_local.locale_id = {locale_id}
           |ORDER BY level""".stripMargin

      SQL(query)
        .on('food_code -> code, 'locale_id -> locale)
        .executeQuery()
        .as(Macro.namedParser[CategoryHeaderRow].*)
        .map(_.asCategoryHeader)
  }

  def categoryParentCategories(code: String, locale: String): Seq[CategoryHeader] = tryWithConnection {
    implicit conn =>
      SQL("""|SELECT code, description, local_description, is_hidden 
             |FROM categories_categories 
             |     JOIN categories ON categories_categories.category_code = code 
             |     LEFT JOIN categories_local ON categories_local.category_code = code AND categories_local.locale_id = {locale_id}
             |WHERE subcategory_code = {category_code}
             |ORDER BY categories_categories.category_code""".stripMargin)
        .on('category_code -> code, 'locale_id -> locale)
        .executeQuery()
        .as(Macro.namedParser[CategoryHeaderRow].*)
        .map(_.asCategoryHeader)
  }

  def categoryAllCategories(code: String, locale: String): Seq[CategoryHeader] = tryWithConnection {
    implicit conn =>
      val query =
        """|WITH RECURSIVE t(code, level) AS (
           |(SELECT category_code AS code, 0 AS level FROM categories_categories WHERE subcategory_code = {category_code} ORDER BY code)
           | UNION ALL
           |(SELECT category_code AS code, level + 1 AS level FROM t JOIN categories_categories ON subcategory_code = code ORDER BY code)
           |)
           |SELECT categories.code, description, local_description, is_hidden 
           |FROM t 
           |     JOIN categories on t.code = categories.code
           |     LEFT JOIN categories_local on t.code = categories_local.category_code AND categories_local.locale_id = {locale_id}
           |ORDER BY level""".stripMargin

      SQL(query)
        .on('category_code -> code, 'locale_id -> locale)
        .executeQuery()
        .as(Macro.namedParser[CategoryHeaderRow].*)
        .map(_.asCategoryHeader)
  }

  def categoryDef(code: String, locale: String): Category = tryWithConnection {
    implicit conn =>
      val psmResults =
        SQL(categoryPortionSizeMethodsQuery).on('category_code -> code, 'locale_id -> locale).executeQuery().as(psmResultRowParser.*)

      val portionSizeMethods = mkPortionSizeMethods(psmResults)

      val categoryQuery =
        """|SELECT categories.version as version, categories_local.version as local_version, code, description, local_description, 
           |       is_hidden, same_as_before_option, ready_meal_option, reasonable_amount 
           |FROM categories 
           |     INNER JOIN categories_attributes ON categories.code = categories_attributes.category_code
           |     LEFT JOIN categories_local ON categories.code = categories_local.category_code AND categories_local.locale_id = {locale_id} 
           |WHERE code = {category_code}""".stripMargin

      val categoryRowParser = Macro.namedParser[CategoryResultRow]

      val result = SQL(categoryQuery).on('category_code -> code, 'locale_id -> locale).executeQuery().as(categoryRowParser.single)

      Category(result.version, result.code, result.description, result.is_hidden,
        InheritableAttributes(result.ready_meal_option, result.same_as_before_option, result.reasonable_amount),
        CategoryLocal(result.local_version, result.local_description, portionSizeMethods))
  }

  case class AsServedResultRow(id: String, description: String, weight: Double, url: String)

  def allAsServedSets(): Seq[AsServedHeader] = tryWithConnection {
    implicit conn =>
      SQL("""SELECT id, description FROM as_served_sets ORDER BY description ASC""").executeQuery().as(Macro.namedParser[AsServedHeader].*)
  }
  
  def asServedDef(id: String): AsServedSet = tryWithConnection {
    implicit conn =>
      val query =
        """|SELECT as_served_sets.id, description, weight, url
         |FROM as_served_sets JOIN as_served_images ON as_served_sets.id = as_served_set_id
         |WHERE as_served_sets.id = {id} ORDER BY as_served_images.id""".stripMargin

      val result = SQL(query).on('id -> id).executeQuery().as(Macro.namedParser[AsServedResultRow].+)

      val images = result.map(row => AsServedImage(row.url, row.weight))

      AsServedSet(result.head.id, result.head.description, images)
  }

  // case class GuideImage (id: String, description: String, weights: Seq[GuideImageWeightRecord])
  // case class GuideImageWeightRecord (description: String, objectId: Integer, weight: Double)

  case class GuideResultRow(image_description: String, object_id: Int, object_description: String, weight: Double)

  def allGuideImages(): Seq[GuideHeader] = tryWithConnection {
    implicit conn =>
      SQL("""SELECT id, description from guide_images ORDER BY description ASC""").executeQuery().as(Macro.namedParser[GuideHeader].*)
  }
  
  def guideDef(id: String): GuideImage = tryWithConnection {
    implicit conn =>
      val query =
        """|SELECT guide_images.description as image_description, object_id, 
         |       guide_image_weights.description as object_description, weight 
         |FROM guide_images JOIN guide_image_weights ON guide_images.id = guide_image_id 
         |WHERE guide_images.id = {id} ORDER BY guide_image_weights.object_id""".stripMargin

      val result = SQL(query).on('id -> id).executeQuery().as(Macro.namedParser[GuideResultRow].+)

      val weights = result.map(row => GuideImageWeightRecord(row.object_description, row.object_id, row.weight))

      GuideImage(id, result.head.image_description, weights)
  }

  case class DrinkwareResultRow(id: String, scale_id: Long, description: String, guide_image_id: String,
    width: Int, height: Int, empty_level: Int, full_level: Int, choice_id: Int, base_image_url: String,
    overlay_image_url: String)

  case class VolumeSampleResultRow(scale_id: Long, fill: Double, volume: Double)
  
  def allDrinkware(): Seq[DrinkwareHeader] = tryWithConnection {
    implicit conn => 
      SQL("""SELECT id, description FROM drinkware_sets ORDER BY description ASC""").executeQuery().as(Macro.namedParser[DrinkwareHeader].*)
  }
  
  def drinkwareDef(id: String): DrinkwareSet = tryWithConnection {
    implicit conn =>
      val drinkwareScalesQuery =
        """|SELECT drinkware_sets.id, drinkware_scales.id as scale_id, description, guide_image_id, 
         |       width, height, empty_level, full_level, choice_id, base_image_url, overlay_image_url
         |FROM drinkware_sets JOIN drinkware_scales ON drinkware_set_id = drinkware_sets.id
         |WHERE drinkware_sets.id = {drinkware_id}
         |ORDER by scale_id""".stripMargin

      val result = SQL(drinkwareScalesQuery).on('drinkware_id -> id).executeQuery().as(Macro.namedParser[DrinkwareResultRow].+)

      val scale_ids = result.map(_.scale_id)

      val drinkwareVolumeSamplesQuery =
        """|SELECT drinkware_scale_id as scale_id, fill, volume 
         |FROM drinkware_volume_samples 
         |WHERE drinkware_scale_id IN ({scale_ids}) ORDER BY scale_id, fill""".stripMargin

      val volume_sample_results = SQL(drinkwareVolumeSamplesQuery).on('scale_ids -> scale_ids).executeQuery().as(Macro.namedParser[VolumeSampleResultRow].+)

      val scales = result.map(r => DrinkScale(r.choice_id, r.base_image_url, r.overlay_image_url, r.width, r.height, r.empty_level, r.full_level,
        VolumeFunction(volume_sample_results.filter(_.scale_id == r.scale_id).map(s => (s.fill, s.volume)))))

      DrinkwareSet(id, result.head.description, result.head.guide_image_id, scales)
  }

  def associatedFoodPrompts(foodCode: String, locale: String): Seq[Prompt] = tryWithConnection {
    implicit conn =>
      val query =
        """SELECT category_code, text, link_as_main, generic_name FROM associated_food_prompts WHERE food_code = {food_code} AND locale_id = {locale_id} ORDER BY id"""

      SQL(query).on('food_code -> foodCode, 'locale_id -> locale).executeQuery().as(Macro.parser[Prompt]("category_code", "text", "link_as_main", "generic_name").*)
  }

  def brandNames(foodCode: String, locale: String): Seq[String] = tryWithConnection {
    implicit conn =>
      SQL("""SELECT name FROM brands WHERE food_code = {food_code} AND locale_id = {locale_id} ORDER BY id""")
        .on('food_code -> foodCode, 'locale_id -> locale)
        .executeQuery()
        .as(str("name").*)
  }

  case class FoodGroupRow(id: Long, description: String, local_description: Option[String])

  def allFoodGroups(locale: String): Seq[FoodGroup] = tryWithConnection {
    implicit conn =>
      SQL("""|SELECT id, description, local_description 
             |FROM food_groups 
             |  LEFT JOIN food_groups_local ON food_groups_local.food_group_id = food_groups.id AND food_groups_local.locale_id = {locale_id}""".stripMargin)
        .on('locale_id -> locale)
        .executeQuery()
        .as(Macro.namedParser[FoodGroupRow].*)
        .map(r => FoodGroup(r.id.toInt, r.description, r.local_description))
  }

  def foodGroup(id: Int, locale: String): Option[FoodGroup] = tryWithConnection {
    implicit conn =>
      SQL("""|SELECT description, local_description 
                        |FROM food_groups 
                        |     LEFT JOIN food_groups_local ON food_groups_local.food_group_id = food_groups.id AND food_groups_local.locale_id = {locale_id}
                        |WHERE id = {id}""".stripMargin)
        .on('id -> id, 'locale_id -> locale)
        .executeQuery()
        .as((str("description") ~ str("local_description").?).singleOpt)
        .map(desc => FoodGroup(id, desc._1, desc._2))
  }

  case class SplitListRow(first_word: String, words: String)

  def splitList(locale: String): SplitList = tryWithConnection {
    implicit conn =>
      val words = SQL("""SELECT words FROM split_words WHERE locale_id={locale}""")
        .on('locale -> locale)
        .executeQuery()
        .as(str("words").single).split("\\s+").toSeq

      val splitList = SQL("""SELECT first_word, words FROM split_list WHERE locale_id={locale}""")
        .on('locale -> locale)
        .executeQuery()
        .as(Macro.namedParser[SplitListRow].*)
        .map {
          case SplitListRow(first_word, words) => (first_word, words.split("\\s+").toSet)
        }.toMap

      SplitList(words, splitList)
  }

  def synsets(locale: String) = tryWithConnection {
    implicit conn =>
      SQL("""SELECT synonyms FROM synonym_sets WHERE locale_id={locale}""")
        .on('locale -> locale)
        .executeQuery()
        .as(str("synonyms").*)
        .map(row => row.split("\\s+").toSet)
  }

  def searchFoods(searchTerm: String, locale: String): Seq[FoodHeader] = tryWithConnection {
    implicit conn =>
      val lowerCaseTerm = searchTerm.toLowerCase

      val query =
        """|SELECT code, description, local_description
           |FROM foods JOIN foods_local ON foods.code = foods_local.food_code 
           |WHERE lower(local_description) LIKE {pattern} OR lower(code) LIKE {pattern} 
           |ORDER BY local_description DESC
           |LIMIT 50""".stripMargin

      SQL(query).on('pattern -> s"%${lowerCaseTerm}%").executeQuery().as(Macro.namedParser[FoodHeaderRow].*).map(_.asFoodHeader)
  }

  def searchCategories(searchTerm: String, locale: String): Seq[CategoryHeader] = tryWithConnection {
    implicit conn =>
      val lowerCaseTerm = searchTerm.toLowerCase

      val query =
        """|SELECT code, description, local_description, is_hidden
           |FROM categories JOIN categories_local ON categories.code = categories_local.category_code
           |WHERE lower(local_description) LIKE {pattern} OR lower(code) LIKE {pattern}
           |ORDER BY local_description DESC
           |LIMIT 50""".stripMargin

      SQL(query).on('pattern -> s"%${lowerCaseTerm}%").executeQuery().as(Macro.namedParser[CategoryHeaderRow].*).map(_.asCategoryHeader)

  }
  
  case class NutrientTableDescRow (id: String, description: String) {
    def asNutrientTable = NutrientTable(id, description)
  }
  
  def nutrientTables(): Seq[NutrientTable] = tryWithConnection {
    implicit conn =>
      val query = """SELECT id, description FROM nutrient_tables ORDER BY id ASC"""
      SQL(query).executeQuery().as(Macro.namedParser[NutrientTableDescRow].*).map(_.asNutrientTable)
  }
  
}