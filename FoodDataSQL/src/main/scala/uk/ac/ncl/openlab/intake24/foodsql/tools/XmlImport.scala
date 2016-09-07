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

package uk.ac.ncl.openlab.intake24.foodsql.tools

import anorm._
import upickle.default._
import java.sql.DriverManager
import scala.xml.XML
import java.io.File
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import java.sql.Connection
import upickle.Invalid
import uk.ac.ncl.openlab.intake24.foodxml.FoodGroupDef
import uk.ac.ncl.openlab.intake24.foodxml.FoodDef
import uk.ac.ncl.openlab.intake24.foodxml.GuideImageDef
import uk.ac.ncl.openlab.intake24.foodxml.BrandDef
import uk.ac.ncl.openlab.intake24.foodxml.DrinkwareDef
import uk.ac.ncl.openlab.intake24.foodxml.PromptDef
import uk.ac.ncl.openlab.intake24.foodxml.CategoryDef
import uk.ac.ncl.openlab.intake24.foodxml.AsServedDef
import java.sql.BatchUpdateException
import org.rogach.scallop.ScallopConf
import java.util.Properties
import java.io.BufferedReader
import java.io.InputStreamReader

import java.util.UUID
import uk.ac.ncl.openlab.intake24.foodsql.Util
import org.postgresql.util.PSQLException
import org.rogach.scallop.ScallopOption
import uk.ac.ncl.openlab.intake24.foodsql.admin.FoodDatabaseAdminImpl

import uk.ac.ncl.openlab.intake24.FoodGroupLocal

import uk.ac.ncl.openlab.intake24.LocalFoodRecord

import uk.ac.ncl.openlab.intake24.NewFood
import uk.ac.ncl.openlab.intake24.services.fooddb.admin.FoodDatabaseAdminService
import uk.ac.ncl.openlab.intake24.services.fooddb.errors.DatabaseError
import uk.ac.ncl.openlab.intake24.NewCategory
import uk.ac.ncl.openlab.intake24.LocalCategoryRecord
import uk.ac.ncl.openlab.intake24.AssociatedFood
import uk.ac.ncl.openlab.intake24.SplitList
import uk.ac.ncl.openlab.intake24.NewLocalFoodRecord
import uk.ac.ncl.openlab.intake24.foodxml.XmlFoodRecord
import uk.ac.ncl.openlab.intake24.FoodGroupMain
import uk.ac.ncl.openlab.intake24.foodxml.XmlCategoryRecord
import uk.ac.ncl.openlab.intake24.AsServedImage
import uk.ac.ncl.openlab.intake24.AsServedSet
import uk.ac.ncl.openlab.intake24.GuideImage
import uk.ac.ncl.openlab.intake24.DrinkwareSet

class XmlImporter(adminService: FoodDatabaseAdminService) {

  val logger = LoggerFactory.getLogger(getClass)

  val defaultLocale = "en_GB"

  private def checkError[E, T](op: String, result: Either[E, T]) = result match {
    case Left(e) => throw new RuntimeException(s"$op failed: ${e.toString()}")
    case _ => logger.info(s"$op successful")
  }

  def importFoodGroups(foodGroups: Seq[FoodGroupMain]) = {
    val baseLocaleData = foodGroups.map {
      g => (g.id -> FoodGroupLocal(Some(g.englishDescription)))
    }.toMap

    checkError("Food groups import ", for (
      _ <- adminService.deleteAllFoodGroups().right;
      _ <- adminService.createFoodGroups(foodGroups).right;
      _ <- adminService.createLocalFoodGroups(baseLocaleData, defaultLocale).right
    ) yield ())
  }

  def importFoods(foods: Seq[XmlFoodRecord], categories: Seq[XmlCategoryRecord], associatedFoods: Map[String, Seq[AssociatedFood]], brandNames: Map[String, Seq[String]]) = {

    val parentCategories = {
      val z = Map[String, Set[String]]()

      categories.foldLeft(z) {
        (map, record) =>
          record.foods.foldLeft(z) {
            (map, foodCode) => map + (foodCode -> (map.getOrElse(foodCode, Set()) + record.code))
          }
      }
    }

    val newFoodRecords = foods.map {
      f => NewFood(f.code, f.description, f.groupCode, f.attributes, parentCategories(f.code).toSeq)
    }

    val newLocalRecords = foods.map {
      f => (f.code -> NewLocalFoodRecord(Some(f.description), false, f.nutrientTableCodes, f.portionSizeMethods, associatedFoods.getOrElse(f.code, Seq()), brandNames.getOrElse(f.code, Seq())))
    }.toMap

    checkError("Foods import", for (
      _ <- adminService.deleteAllFoods().right;
      _ <- adminService.createFoods(newFoodRecords).right;
      _ <- adminService.createLocalFoods(newLocalRecords, defaultLocale).right
    ) yield ())
  }

  def importCategories(categories: Seq[XmlCategoryRecord]) = {

    val newCategoryRecords = categories.map {
      c => NewCategory(c.code, c.description, c.isHidden, c.attributes)
    }

    val newLocalRecords = categories.map {
      c => (c.code -> LocalCategoryRecord(None, Some(c.description), c.portionSizeMethods))
    }.toMap

    checkError("Categories import", for (
      _ <- adminService.deleteAllCategories().right;
      _ <- adminService.createCategories(newCategoryRecords).right;
      _ <- adminService.createLocalCategories(newLocalRecords, defaultLocale).right
    ) yield ())
  }

  def importAsServedSets(asServed: Seq[AsServedSet]) =
    checkError("As served sets import", for (
      _ <- adminService.deleteAllAsServedSets().right;
      _ <- adminService.createAsServedSets(asServed).right
    ) yield ())

  private case class ImageMapArea(id: Int, coords: Seq[Double])
  private case class ImageMapRecord(navigation: Seq[Seq[Int]], areas: Seq[ImageMapArea])

  private def parseImageMaps(imageMapsPath: String) = {
    def parseImageMap(file: File) = {
      logger.debug("Importing image map from " + file.getName)
      val json = scala.io.Source.fromFile(file).mkString
      read[ImageMapRecord](json)
    }

    ((new File(imageMapsPath)).listFiles() match {
      case null => {
        logger.warn("Image maps path does not exist or is not a directory")
        Array[File]()
      }
      case files => files
    }).filter(_.getName.endsWith(".imagemap")).map(parseImageMap)
  }

  def importImageMaps(imageMaps: Seq[ImageMapRecord]) = ???

  def importGuideImages(guideImages: Seq[GuideImage]) = {
    checkError("Guide image import", for (
      _ <- adminService.deleteAllGuideImages().right;
      _ <- adminService.createGuideImages(guideImages).right
    ) yield ())
  }

  def importDrinkwareSets(drinkwareSets: Seq[DrinkwareSet]) = {
    checkError("Drinkware sets import", for (
      _ <- adminService.deleteAllDrinkwareSets().right;
      _ <- adminService.createDrinkwareSets(drinkwareSets).right
    ) yield ())
  }

  def parseAssociatedFoods(categories: Seq[XmlCategoryRecord], promptsPath: String): Map[String, Seq[AssociatedFood]] = {
    logger.debug("Loading associated food prompts from " + promptsPath)

    val prompts = PromptDef.parseXml(XML.load(promptsPath))

    logger.debug("Indexing foods and categories for associated type resolution" + promptsPath)

    val categoryCodes = categories.map(_.code).toSet

    logger.debug("Resolving associated food/category types")

    prompts.mapValues {
      _.map {
        prompt =>
          if (categoryCodes.contains(prompt.category)) {
            logger.info(s"Resolved ${prompt.category} as category")
            AssociatedFood(Right(prompt.category), prompt.promptText, prompt.linkAsMain, prompt.genericName)
          } else {
            logger.info(s"Resolved ${prompt.category} as food")
            AssociatedFood(Left(prompt.category), prompt.promptText, prompt.linkAsMain, prompt.genericName)
          }
      }
    }
  }

  def importSplitList(path: String) {

    logger.info("Loading split list from " + path)
    val lines = scala.io.Source.fromFile(path).getLines().toSeq

    val splitWords = lines.head.split("\\s+")

    val keepPairs = lines.tail.map {
      line =>
        val words = line.split("\\s+")

        (words.head, words.tail.toSet)

    }.toMap

    checkError("Split list import", for (
      _ <- adminService.deleteSplitList(defaultLocale).right;
      _ <- adminService.createSplitList(SplitList(splitWords, keepPairs), defaultLocale).right
    ) yield ())
  }

  def importSynonymSets(path: String) = {
    logger.info("Loading synonym sets from " + path)
    val synsets = scala.io.Source.fromFile(path).getLines().toSeq.map(_.split("\\s+").toSet)

    checkError("Synonym sets import", for (
      _ <- adminService.deleteSynsets(defaultLocale).right;
      _ <- adminService.createSynsets(synsets, defaultLocale).right
    ) yield ())
  }

  def importXmlData(dataDirectory: String) = {

    val foodGroups = FoodGroupDef.parseXml(XML.load(dataDirectory + File.separator + "food-groups.xml"))
    val foods = FoodDef.parseXml(XML.load(dataDirectory + File.separator + "foods.xml"))
    val categories = CategoryDef.parseXml(XML.load(dataDirectory + File.separator + "categories.xml"))
    val associatedFoods = parseAssociatedFoods(categories, dataDirectory + File.separator + "prompts.xml")
    val brands = BrandDef.parseXml(XML.load(dataDirectory + File.separator + "brands.xml"))
    val asServedSets = AsServedDef.parseXml(XML.load(dataDirectory + File.separator + "as-served.xml")).values.toSeq
    val guideImages = GuideImageDef.parseXml(XML.load(dataDirectory + File.separator + "guide.xml")).values.toSeq
    val imageMaps = parseImageMaps(dataDirectory + File.separator + "CompiledImageMaps")
    val drinkwareSets = DrinkwareDef.parseXml(XML.load(dataDirectory + File.separator + "drinkware.xml")).values.toSeq

    importCategories(categories)

    importFoodGroups(foodGroups)
    importAsServedSets(asServedSets)
    importGuideImages(guideImages)
    /// importImageMaps(imageMaps)
    importDrinkwareSets(drinkwareSets)
    
    importFoods(foods, categories, associatedFoods, brands)

    importSplitList(dataDirectory + File.separator + "split_list")
    importSynonymSets(dataDirectory + File.separator + "synsets")
  }
}

trait Options extends ScallopConf {
  version("Intake24 XML to SQL food database migration tool 16.8")

  val xmlPath = opt[String](required = true, noshort = true)
}

object XmlImport extends App with WarningMessage with DatabaseConnection {

  val logger = LoggerFactory.getLogger(getClass)

  val options = new ScallopConf(args) with Options with DatabaseOptions

  options.afterInit()

  displayWarningMessage("THIS WILL DESTROY ALL FOOD AND CATEGORY RECORDS IN THE DATABASE!")

  val dataSource = getDataSource(options)

  val adminService = new FoodDatabaseAdminImpl(dataSource)

  implicit val dbConn = dataSource.getConnection

  val importer = new XmlImporter(adminService)

  importer.importXmlData(options.xmlPath())
}
