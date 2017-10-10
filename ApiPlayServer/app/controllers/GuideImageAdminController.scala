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

package controllers

import javax.inject.Inject

import io.circe.generic.auto._
import parsers.{FormDataUtil, JsonBodyParser}
import play.api.libs.Files.TemporaryFile
import play.api.mvc.MultipartFormData.FilePart
import play.api.mvc.{BaseController, ControllerComponents, Result}
import security.Intake24RestrictedActionBuilder
import uk.ac.ncl.openlab.intake24.api.shared.{ErrorDescription, NewGuideImageRequest, NewImageMapRequest, NewImageMapWithObjectsRequest}
import uk.ac.ncl.openlab.intake24.services.fooddb.admin._
import uk.ac.ncl.openlab.intake24.services.fooddb.images.{AWTImageMap, ImageAdminService, ImageStorageService}

import scala.concurrent.{ExecutionContext, Future}

class GuideImageAdminController @Inject()(guideImageAdminService: GuideImageAdminService,
                                          imageMapsAdminService: ImageMapsAdminService,
                                          imageAdminService: ImageAdminService,
                                          imageStorage: ImageStorageService,
                                          foodAuthChecks: FoodAuthChecks,
                                          rab: Intake24RestrictedActionBuilder,
                                          jsonBodyParser: JsonBodyParser,
                                          val controllerComponents: ControllerComponents,
                                          implicit val executionContext: ExecutionContext) extends BaseController
  with ImageOrDatabaseServiceErrorHandler with FormDataUtil {

  import ImageAdminService.WrapDatabaseError

  def listGuideImages() = rab.restrictAccess(foodAuthChecks.canReadPortionSizeMethods) {
    Future {
      translateDatabaseResult(guideImageAdminService.listGuideImages().map { images =>
        images.map(img => img.copy(path = imageStorage.getUrl(img.path)))
      })
    }
  }

  def getGuideImage(id: String) = rab.restrictAccess(foodAuthChecks.canReadPortionSizeMethods) {
    Future {
      translateDatabaseResult(guideImageAdminService.getGuideImage(id))
    }
  }

  def getGuideImageFull(id: String) = rab.restrictAccess(foodAuthChecks.canReadPortionSizeMethods) {
    Future {
      translateDatabaseResult(guideImageAdminService.getFullGuideImage(id))
    }
  }

  def patchGuideImageMeta(id: String) = rab.restrictAccess(foodAuthChecks.canWritePortionSizeMethods)(jsonBodyParser.parse[GuideImageMeta]) {
    request =>
      Future {
        translateDatabaseResult(guideImageAdminService.patchGuideImageMeta(id, request.body))
      }
  }

  def updateGuideSelectionImage(id: String, selectionImageId: Long) = rab.restrictAccess(foodAuthChecks.canWritePortionSizeMethods) {
    Future {
      translateDatabaseResult(guideImageAdminService.updateGuideSelectionImage(id, selectionImageId))
    }
  }

  def createGuideImage() = rab.restrictAccess(foodAuthChecks.canWritePortionSizeMethods)(jsonBodyParser.parse[NewGuideImageRequest]) {
    request =>
      Future {

        val weights = request.body.objectWeights.map {
          case (k, v) => (k.toLong, v)
        }

        val result = for (
          sourceId <- imageMapsAdminService.getImageMapBaseImageSourceId(request.body.imageMapId).wrapped.right;
          selectionImageDescriptor <- imageAdminService.processForSelectionScreen(s"guide/${request.body.id}/selection", sourceId).right;
          _ <- guideImageAdminService.createGuideImages(Seq(NewGuideImageRecord(request.body.id, request.body.description, request.body.imageMapId, selectionImageDescriptor.id, weights))).wrapped.right) yield ()

        translateImageServiceAndDatabaseResult(result)
      }
  }

  def uploadGuideImage() = rab.restrictAccess(foodAuthChecks.canWritePortionSizeMethods)(parse.multipartFormData) {
    request =>
      Future {
        val result = for (
          baseImage <- getFile("baseImage", request.body).right;
          sourceKeywords <- getOptionalMultipleData("baseImageKeywords", request.body).right;
          params <- getParsedData[NewImageMapRequest]("imageMapParameters", request.body).right;
          _ <- createGuideImageMap(baseImage, sourceKeywords, params, request.subject.userId.toString).right // FIXME: better uploader string
        ) yield ()

        result match {
          case Left(badResult) => badResult
          case Right(()) => Ok
        }
      }
  }

  private def createGuideImageMap(baseImage: FilePart[TemporaryFile], keywords: Seq[String], params: NewImageMapRequest, uploader: String): Either[Result, Unit] =
    translateImageServiceAndDatabaseError(
      for (
        baseImageSourceRecord <- imageAdminService.uploadSourceImage(ImageAdminService.getSourcePathForImageMap(params.id, baseImage.filename), baseImage.ref.path, keywords, uploader).right;
        processedBaseImageDescriptor <- imageAdminService.processForImageMapBase(params.id, baseImageSourceRecord.id).right;
        _ <- {
          imageMapsAdminService.createImageMaps(Seq(NewImageMapRecord(params.id, params.description, processedBaseImageDescriptor.id, Nil, Map.empty)))
        }.wrapped.right
      ) yield ())

}
