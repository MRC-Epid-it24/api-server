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

package modules

import java.util.concurrent.ForkJoinPool
import cache._
import com.google.inject.{AbstractModule, Provides}

import javax.inject.{Named, Singleton}
import javax.sql.DataSource
import org.http4s.Uri
import play.api.db.Database
import play.api.{Configuration, Environment}
import play.db.NamedDatabase
import scheduled._
import scheduled.notificationSender.{NotificationSender, NotificationSenderImpl}
import security.captcha.{AsyncCaptchaService, GoogleRecaptchaImpl}
import sms.{SMSService, TwilioSMSImpl}
import uk.ac.ncl.openlab.intake24.foodsql.admin._
import uk.ac.ncl.openlab.intake24.foodsql.demographicGroups._
import uk.ac.ncl.openlab.intake24.foodsql.foodindex.FoodIndexDataImpl
import uk.ac.ncl.openlab.intake24.foodsql.images.ImageDatabaseServiceSqlImpl
import uk.ac.ncl.openlab.intake24.foodsql.recipes.RecipesAttributeCacheImpl
import uk.ac.ncl.openlab.intake24.foodsql.user.{FoodCompositionServiceImpl, _}
import uk.ac.ncl.openlab.intake24.services.fooddb.admin._
import uk.ac.ncl.openlab.intake24.services.fooddb.demographicgroups._
import uk.ac.ncl.openlab.intake24.services.fooddb.images._
import uk.ac.ncl.openlab.intake24.services.fooddb.user._
import uk.ac.ncl.openlab.intake24.services.foodindex._
import uk.ac.ncl.openlab.intake24.services.foodindex.arabic.{FoodIndexImpl_ar_AE, SplitterImpl_ar_AE}
import uk.ac.ncl.openlab.intake24.services.foodindex.danish.{FoodIndexImpl_da_DK, SplitterImpl_da_DK}
import uk.ac.ncl.openlab.intake24.services.foodindex.english._
import uk.ac.ncl.openlab.intake24.services.foodindex.portuguese.{FoodIndexImpl_pt_PT, SplitterImpl_pt_PT}
import uk.ac.ncl.openlab.intake24.services.nutrition.{DefaultNutrientMappingServiceImpl, FoodCompositionService, NutrientMappingService}
import uk.ac.ncl.openlab.intake24.services.systemdb.admin._
import uk.ac.ncl.openlab.intake24.services.systemdb.notifications.NotificationScheduleDataService
import uk.ac.ncl.openlab.intake24.services.systemdb.user._
import uk.ac.ncl.openlab.intake24.services.systemdb.uxEvents.UxEventsDataService
import uk.ac.ncl.openlab.intake24.services.{NdnsCompoundFoodGroupsService, RecipesAttributeCache}
import uk.ac.ncl.openlab.intake24.shorturls.ShortUrlsHttpClientConfig
import uk.ac.ncl.openlab.intake24.systemsql.admin._
import uk.ac.ncl.openlab.intake24.systemsql.notifications.NotificationScheduleDataServiceImpl
import uk.ac.ncl.openlab.intake24.systemsql.user._
import uk.ac.ncl.openlab.intake24.systemsql.uxEvents.UxEventsDataServiceImpl

import java.awt.Color
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class Intake24ServicesModule extends AbstractModule {
  @Provides
  @Singleton
  def foodIndexes(foodIndexDataService: FoodIndexDataService, configuration: Configuration): Map[String, FoodIndex] = {

    val reloadPeriod = configuration.get[Int]("intake24.foodIndex.reloadPeriodMinutes").minutes
    val enabledLocales = configuration.get[Seq[String]]("intake24.foodIndex.enabledLocales")

    // This could be done using DI, but not sure if holding an injector instance for auto reloading
    // is a good idea
    def createIndex(localeId: String) = localeId match {
      case "en_GB" => new FoodIndexImpl_en_GB(foodIndexDataService)
      case "pt_PT" => new FoodIndexImpl_pt_PT(foodIndexDataService)
      case "da_DK" => new FoodIndexImpl_da_DK(foodIndexDataService)
      case "ar_AE" => new FoodIndexImpl_ar_AE(foodIndexDataService)
      case "en_NZ" => new FoodIndexImpl_en_NZ(foodIndexDataService)
      case "en_NZ_2" => new FoodIndexImpl_en_NZ_2(foodIndexDataService)
      case "en_GB_gf" => new FoodIndexImpl_en_GB_gf(foodIndexDataService)
      case "en_GB_simple" => new FoodIndexImpl_en_GB_simple(foodIndexDataService)
      case "en_GB_sa" => new FoodIndexImpl_en_GB_SA(foodIndexDataService)
      case "en_IN" => new FoodIndexImpl_en_IN(foodIndexDataService)
      case "en_AU" => new FoodIndexImpl_en_AU(foodIndexDataService)
      case "SABv1" => new FoodIndexImpl_SABv1(foodIndexDataService)
      case "NDNSv1" => new FoodIndexImpl_NDNSv1(foodIndexDataService)
      case "BHSAv1" => new FoodIndexImpl_BHSAv1(foodIndexDataService)
      case "UKSAv2" => new FoodIndexImpl_UKSAv2(foodIndexDataService)
      case "UK_current" => new FoodIndexImpl_UK_current(foodIndexDataService)
      case "UK_V2_2022" => new FoodIndexImpl_UK_V2_2022(foodIndexDataService)
      case "UK_V3_2023" => new FoodIndexImpl_UK_V3_2023(foodIndexDataService)
      case "en_FJ" => new FoodIndexImpl_en_FJ(foodIndexDataService)
      case "NZ_V3_2023" => new FoodIndexImpl_NZ_V3_2023(foodIndexDataService)
    }

    val globalReloadPeriod = reloadPeriod * enabledLocales.size

    def buildMap(acc: Map[String, FoodIndex],
                 remaining: List[String],
                 delay: Duration): Map[String, FoodIndex] = remaining match {
      case Nil => acc
      case locale :: locales =>
        buildMap(acc + (locale -> new AutoReloadIndex(() => createIndex(locale), delay, globalReloadPeriod, locale)),
          locales, delay + reloadPeriod)
    }

    buildMap(Map(), enabledLocales.toList, reloadPeriod)
  }

  @Provides
  @Singleton
  def recipesAttributeIndex(@Named("intake24_foods") dataSource: DataSource, configuration: Configuration): RecipesAttributeCache = {
    val reloadPeriod = configuration.get[Int]("intake24.foodIndex.reloadPeriodMinutes").minutes

    new AutoReloadRecipesCache(() => new RecipesAttributeCacheImpl(dataSource), reloadPeriod, reloadPeriod)
  }

  @Provides
  @Singleton
  def foodDescriptionSplitters(foodIndexDataService: FoodIndexDataService, configuration: Configuration): Map[String, Splitter] = {

    val enabledLocales = configuration.get[Seq[String]]("intake24.foodIndex.enabledLocales")

    def createSplitter(localeId: String) = localeId match {
      case "en_GB" => new SplitterImpl_en_GB(foodIndexDataService)
      case "pt_PT" => new SplitterImpl_pt_PT(foodIndexDataService)
      case "da_DK" => new SplitterImpl_da_DK(foodIndexDataService)
      case "ar_AE" => new SplitterImpl_ar_AE(foodIndexDataService)
      case "en_NZ" => new SplitterImpl_en_NZ(foodIndexDataService)
      case "en_NZ_2" => new SplitterImpl_en_NZ(foodIndexDataService)
      case "en_GB_gf" => new SplitterImpl_en_GB_gf(foodIndexDataService)
      case "en_GB_simple" => new SplitterImpl_en_GB_simple(foodIndexDataService)
      case "en_GB_sa" => new SplitterImpl_en_GB(foodIndexDataService)
      case "en_IN" => new SplitterImpl_en_IN(foodIndexDataService)
      case "en_AU" => new SplitterImpl_en_AU(foodIndexDataService)
      case "SABv1" => new SplitterImpl_SABv1(foodIndexDataService)
      case "NDNSv1" => new SplitterImpl_NDNSv1(foodIndexDataService)
      case "BHSAv1" => new SplitterImpl_BHSAv1(foodIndexDataService)
      case "UKSAv2" => new SplitterImpl_UKSAv2(foodIndexDataService)
      case "UK_current" => new SplitterImpl_UK_current(foodIndexDataService)
      case "UK_V2_2022" => new SplitterImpl_UK_V2_2022(foodIndexDataService)
      case "UK_V3_2023" => new SplitterImpl_UK_V3_2023(foodIndexDataService)
      case "en_FJ" => new SplitterImpl_en_FJ(foodIndexDataService)
      case "NZ_V3_2023" => new SplitterImpl_NZ_V3_2023(foodIndexDataService)
    }

    enabledLocales.foldLeft(Map[String, Splitter]()) {
      case (acc, locale) => acc + (locale -> createSplitter(locale))
    }
  }


  @Provides
  @Named("intake24_system")
  def systemDataSource(@NamedDatabase("intake24_system") db: Database) = db.dataSource

  @Provides
  @Named("intake24_foods")
  def foodsDataSource(@NamedDatabase("intake24_foods") db: Database) = db.dataSource

  @Provides
  @Singleton
  def imageProcessorSettings(configuration: Configuration): ImageProcessorSettings = {

    val source = SourceImageSettings(
      configuration.get[Int]("intake24.images.processor.source.thumbnailWidth"),
      configuration.get[Int]("intake24.images.processor.source.thumbnailHeight"))

    val asServed = AsServedImageSettings(
      configuration.get[Int]("intake24.images.processor.asServed.mainImageWidth"),
      configuration.get[Int]("intake24.images.processor.asServed.mainImageHeight"),
      configuration.get[Int]("intake24.images.processor.asServed.thumbnailWidth"))

    val selection = SelectionImageSettings(
      configuration.get[Int]("intake24.images.processor.selectionScreen.width"),
      configuration.get[Int]("intake24.images.processor.selectionScreen.height"))

    val imageMaps = ImageMapSettings(
      configuration.get[Int]("intake24.images.processor.imageMaps.baseImageWidth"),
      configuration.get[Double]("intake24.images.processor.imageMaps.outlineStrokeWidth"),
      (configuration.get[Double]("intake24.images.processor.imageMaps.outlineColor.r"),
        configuration.get[Double]("intake24.images.processor.imageMaps.outlineColor.g"),
        configuration.get[Double]("intake24.images.processor.imageMaps.outlineColor.b")),
      configuration.get[Double]("intake24.images.processor.imageMaps.outlineBlurStrength"))

    val slidingScaleSettings = SlidingScaleSettings(
      configuration.get[Int]("intake24.images.processor.slidingScale.baseImageWidth"),
      new Color(
      configuration.get[Double]("intake24.images.processor.slidingScale.fillColor.r").toFloat,
        configuration.get[Double]("intake24.images.processor.slidingScale.fillColor.g").toFloat,
        configuration.get[Double]("intake24.images.processor.slidingScale.fillColor.b").toFloat,
        configuration.get[Double]("intake24.images.processor.slidingScale.fillColor.a").toFloat),
      configuration.get[Double]("intake24.images.processor.slidingScale.blurStrength"))

    val commandSearchPath = configuration.getOptional[String]("intake24.images.processor.commandSearchPath")

    val command = configuration.getOptional[Seq[String]]("intake24.images.processor.command").getOrElse(Seq("magick", "convert"))

    ImageProcessorSettings(commandSearchPath, command, source, selection, asServed, imageMaps, slidingScaleSettings)
  }

  // Custom execution context for long-running blocking tasks (data export etc.)
  @Provides
  @Named("intake24")
  @Singleton
  def longTasksExecutionContext(configuration: Configuration): ExecutionContext = {
    val maxThreads = configuration.get[Int]("intake24.longTasksContext.maxThreads")
    ExecutionContext.fromExecutor(new ForkJoinPool(maxThreads))
  }

  @Provides
  @Singleton
  def shortUrlsConfig(configuration: Configuration): ShortUrlsHttpClientConfig = {
    ShortUrlsHttpClientConfig(Uri.unsafeFromString(configuration.get[String]("intake24.shortUrlServiceUrl")))
  }

  override def configure() = {
    // Utility services

    bind(classOf[EnglishWordOps]).to(classOf[EnglishWordOpsPlingImpl])

    // Basic admin services -- uncached

    bind(classOf[CategoriesAdminService]).annotatedWith(classOf[BasicImpl]).to(classOf[CategoriesAdminImpl])
    bind(classOf[FoodsAdminService]).annotatedWith(classOf[BasicImpl]).to(classOf[FoodsAdminImpl])
    bind(classOf[LocalesAdminService]).annotatedWith(classOf[BasicImpl]).to(classOf[LocalesAdminImpl])

    bind(classOf[CategoriesAdminService]).to(classOf[ObservableCategoriesAdminServiceImpl])
    bind(classOf[FoodsAdminService]).to(classOf[ObservableFoodsAdminServiceImpl])
    bind(classOf[LocalesAdminService]).to(classOf[ObservableLocalesAdminServiceImpl])

    bind(classOf[UserAdminService]).to(classOf[UserAdminImpl])
    bind(classOf[SurveyAdminService]).to(classOf[SurveyAdminImpl])
    bind(classOf[DataExportService]).to(classOf[DataExportImpl])

    // User facing services

    bind(classOf[AsServedSetsAdminService]).to(classOf[AsServedSetsAdminImpl])
    bind(classOf[AssociatedFoodsAdminService]).to(classOf[AssociatedFoodsAdminImpl])
    bind(classOf[DrinkwareAdminService]).to(classOf[DrinkwareAdminImpl])
    bind(classOf[FoodBrowsingAdminService]).to(classOf[FoodBrowsingAdminImpl])
    bind(classOf[FoodGroupsAdminService]).to(classOf[FoodGroupsAdminImpl])
    bind(classOf[GuideImageAdminService]).to(classOf[GuideImageAdminImpl])
    bind(classOf[NutrientTablesAdminService]).to(classOf[NutrientTablesAdminImpl])
    bind(classOf[QuickSearchService]).to(classOf[QuickSearchAdminImpl])
    bind(classOf[ImageMapsAdminService]).to(classOf[ImageMapsAdminImpl])
    bind(classOf[FoodCompositionService]).to(classOf[FoodCompositionServiceImpl])
    bind(classOf[NutrientMappingService]).to(classOf[DefaultNutrientMappingServiceImpl])

    bind(classOf[SurveyService]).to(classOf[SurveyServiceImpl])

    // Observable admin services for higher-level cached services

    bind(classOf[ObservableFoodsAdminService]).to(classOf[ObservableFoodsAdminServiceImpl])
    bind(classOf[ObservableCategoriesAdminService]).to(classOf[ObservableCategoriesAdminServiceImpl])
    bind(classOf[ObservableLocalesAdminService]).to(classOf[ObservableLocalesAdminServiceImpl])

    bind(classOf[ImageDatabaseService]).to(classOf[ImageDatabaseServiceSqlImpl])
    bind(classOf[ImageAdminService]).to(classOf[ImageAdminServiceDefaultImpl])

    bind(classOf[ImageProcessor]).to(classOf[ImageProcessorIM])

    // Admin services -- cached

    bind(classOf[ProblemCheckerService]).to(classOf[CachedProblemChecker])

    // Food index service

    bind(classOf[FoodIndexDataService]).to(classOf[FoodIndexDataImpl])

    // User food database service

    bind(classOf[FoodDataService]).to(classOf[FoodDataServiceImpl])
    bind(classOf[FoodPopularityService]).to(classOf[FoodPopularityServiceImpl])
    bind(classOf[FoodBrowsingService]).to(classOf[FoodBrowsingServiceImpl])
    bind(classOf[AsServedSetsService]).to(classOf[AsServedSetsServiceImpl])
    bind(classOf[GuideImageService]).to(classOf[GuideImageServiceImpl])
    bind(classOf[AssociatedFoodsService]).to(classOf[AssociatedFoodsServiceImpl])
    bind(classOf[DrinkwareService]).to(classOf[DrinkwareServiceImpl])
    bind(classOf[BrandNamesService]).to(classOf[BrandNamesServiceImpl])
    bind(classOf[ImageMapService]).to(classOf[ImageMapServiceImpl])

    bind(classOf[ClientErrorService]).to(classOf[ClientErrorServiceImpl])

    // SMS serviceUser

    bind(classOf[SMSService]).to(classOf[TwilioSMSImpl])

    // Captcha

    bind(classOf[AsyncCaptchaService]).to(classOf[GoogleRecaptchaImpl])

    // Error digest service

    bind(classOf[ErrorDigestSender]).to(classOf[ErrorDigestSenderImpl]).asEagerSingleton()

    // Demographic service
    bind(classOf[DemographicGroupsService]).to(classOf[DemographicGroupsServiceImpl])

    bind(classOf[PhysicalActivityLevelService]).to(classOf[PhysicalActivityLevelImpl])

    bind(classOf[UserPhysicalDataService]).to(classOf[UserPhysicalDataServiceImpl])

    bind(classOf[SigninLogService]).to(classOf[SigninLogImpl])

    bind(classOf[UsersSupportService]).to(classOf[UsersSupportServiceImpl])

    // Ux Events
    bind(classOf[UxEventsDataService]).to(classOf[UxEventsDataServiceImpl])

    // User notifications
    bind(classOf[NotificationScheduleDataService]).to(classOf[NotificationScheduleDataServiceImpl])
    bind(classOf[NotificationSender]).to(classOf[NotificationSenderImpl]).asEagerSingleton()

    // User sessions
    bind(classOf[UserSessionDataService]).to(classOf[UserSessionDataServiceImpl])

    bind(classOf[NdnsCompoundFoodGroupsService]).to(classOf[NdnsCompoundFoodGroupsImpl])
    bind(classOf[FeedbackDataService]).to(classOf[FeedbackDataImpl])


  }


}
