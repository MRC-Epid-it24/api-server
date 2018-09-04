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

package uk.ac.ncl.openlab.intake24.services.dataexport.guice

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.google.inject.{AbstractModule, Provides}
import javax.inject.Singleton
import play.api.Configuration

class AmazonWebServicesModule extends AbstractModule {

  def configure() = {

  }

  @Provides
  @Singleton
  def createS3client(config: Configuration): AmazonS3 = {
    val profileName = config.getOptional[String]("intake24.S3.profileName").getOrElse("default")
    AmazonS3ClientBuilder.standard()
      .withCredentials(new ProfileCredentialsProvider(profileName)).build()
  }

}
