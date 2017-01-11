package uk.ac.ncl.openlab.intake24.api.client

import uk.ac.ncl.openlab.intake24.api.shared.ErrorDescription
import upickle.default._

import scalaj.http.HttpResponse

trait ApiResponseParser {

  private def getResponseBody(response: HttpResponse[String]): Either[ApiError, String] = {
    if (response.code == 200) {
      Right(response.body)
    } else {
      try {
        val desc = read[ErrorDescription](response.body)
        Left(ApiError.RequestFailed(response.code, desc.errorCode, desc.errorMessage))
      } catch {
        case e: Throwable => Left(ApiError.ErrorParseFailed(response.code, e))
      }
    }
  }

  protected def parseApiResponseDiscardBody(response: HttpResponse[String]): Either[ApiError, Unit] = getResponseBody(response).right.map(_ => ())

  protected def parseApiResponse[T](response: HttpResponse[String])(implicit reader: Reader[T]): Either[ApiError, T] =
    getResponseBody(response) match {
      case Right(body) =>
        try {
          Right(read[T](body))
        } catch {
          case e: Throwable => Left(ApiError.ResultParseFailed(e))
        }
      case Left(error) => Left(error)
    }
}