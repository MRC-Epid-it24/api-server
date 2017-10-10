package uk.ac.ncl.openlab.intake24.services.fooddb.admin

import uk.ac.ncl.openlab.intake24.GuideHeader
import uk.ac.ncl.openlab.intake24.errors.{DependentCreateError, DependentUpdateError, UnexpectedDatabaseError, UpdateError}
import uk.ac.ncl.openlab.intake24.services.fooddb.user.GuideImageService

case class NewGuideImageRecord(id: String, description: String, imageMapId: String, selectionImageId: Long, objectWeights: Map[Long, Double])

case class GuideImageFull(meta: GuideImageMeta, path: String, objects: Seq[GuideImageMapObject])

case class GuideImageMeta(id: String, description: String)

case class GuideImageMapObject(id: Option[Long], weight: Double, description: String, navigationIndex: Int, outlineCoordinates: Seq[Double])

trait GuideImageAdminService extends GuideImageService {

  def listGuideImages(): Either[UnexpectedDatabaseError, Seq[GuideHeader]]

  def deleteAllGuideImages(): Either[UnexpectedDatabaseError, Unit]

  def createGuideImages(guideImages: Seq[NewGuideImageRecord]): Either[DependentCreateError, Unit]

  def updateGuideSelectionImage(id: String, selectionImageId: Long): Either[DependentUpdateError, Unit]

  def getFullGuideImage(id: String): Either[DependentUpdateError, GuideImageFull]

  def patchGuideImageMeta(id: String, meta: GuideImageMeta): Either[UpdateError, GuideImageMeta]

  def patchGuideImageObjects(id: String, objects: Seq[GuideImageMapObject]): Either[UpdateError, Seq[GuideImageMapObject]]
}
