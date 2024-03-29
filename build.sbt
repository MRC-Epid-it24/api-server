/*
This file is part of Intake24.

Copyright 2015, 2016, 2017 Newcastle University.

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

import ResolveInternalDependencies._

Global / dependencyCheckFormats := Seq("HTML", "JSON")

lazy val commonSettings = Seq(
  version := "3.33.0-SNAPSHOT",
  scalaVersion := "2.12.14",
  publishArtifact in(Compile, packageDoc) := false,
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
)

lazy val scalaHttpJVM = project.in(file("RosHTTP"))

lazy val apiSharedJVM = project.in(file("api-shared"))

lazy val apiClientJVM = project.in(file("api-client"))

lazy val sharedTypes = Project(id = "sharedTypes", base = file("SharedTypes")).settings(commonSettings: _*)

lazy val infiauto = Project(id = "infiauto", base = file("infiauto")).settings(commonSettings: _*)

lazy val phraseSearch = Project(id = "phrasesearch", base = file("PhraseSearch")).dependsOn(infiauto).settings(commonSettings: _*)

lazy val gwtShared = Project(id = "gwtShared", base = file("ClientShared")).settings(commonSettings: _*)


lazy val commonSql = Project(id = "commonSql", base = file("CommonSQL")).settings(commonSettings: _*).dependsOn(sharedTypes)


lazy val pairwiseAssociationRules = Project(id = "pairwiseAssociationRules", base = file("pairwise-association-rules")).settings(commonSettings: _*)

lazy val systemDataServices =
  Project(id = "systemDataServices", base = file("SystemDataServices")).dependsOn(apiSharedJVM, gwtShared, sharedTypes, pairwiseAssociationRules).settings(commonSettings: _*)

lazy val systemDataSql =
  Project(id = "systemDataSql", base = file("SystemDataSQL")).dependsOn(commonSql, systemDataServices % "compile->compile;test->test", pairwiseAssociationRules).settings(commonSettings: _*)


lazy val ptStemmer = Project(id = "ptStemmer", base = file("PTStemmer-Java"))

lazy val foodDataServices =
  Project(id = "foodDataServices", base = file("FoodDataServices")).dependsOn(apiSharedJVM, sharedTypes, phraseSearch, ptStemmer).settings(commonSettings: _*)

lazy val foodDataXml = Project(id = "foodDataXml", base = file("FoodDataXML")).dependsOn(foodDataServices).settings(commonSettings: _*)

lazy val nutrientsCsv = Project(id = "nutrientsCsv", base = file("NutrientsCSV")).dependsOn(sharedTypes, foodDataServices).settings(commonSettings: _*)

lazy val foodDataSql = Project(id = "foodDataSql", base = file("FoodDataSQL")).dependsOn(commonSql, foodDataServices % "compile->compile;test->test", foodDataXml, nutrientsCsv).settings(commonSettings: _*)

lazy val databaseTools =
  Project(id = "databaseTools", base = file("DatabaseTools")).dependsOn(commonSql, imageProcessorIM, systemDataSql, foodDataXml, foodDataSql, apiSharedJVM).settings(commonSettings: _*)

lazy val imageStorageLocal = Project(id = "imageStorageLocal", base = file("ImageStorageLocal")).dependsOn(foodDataServices).settings(commonSettings: _*)

lazy val imageStorageS3 = Project(id = "imageStorageS3", base = file("ImageStorageS3")).dependsOn(foodDataServices).settings(commonSettings: _*)

lazy val imageProcessorIM = Project(id = "imageProcessorIM", base = file("ImageProcessorIM")).dependsOn(foodDataServices).settings(commonSettings: _*)

lazy val standardize = Project(id = "standardize", base = file("standardize")).settings(commonSettings: _*)

lazy val foodSubstRecommender = Project(id = "foodSubstRecommender", base = file("FoodSubstRecommender")).dependsOn(foodDataServices, foodDataSql, standardize).settings(commonSettings: _*)

lazy val playUtils = project.in(file("PlayUtils")).dependsOn(sharedTypes, apiSharedJVM)

lazy val playSecurity = project.in(file("PlaySecurity")).dependsOn(playUtils, systemDataSql, apiSharedJVM)

lazy val apiPlayServer =
  Project(id = "apiPlayServer", base = file("ApiPlayServer")).enablePlugins(PlayScala, SystemdPlugin, JDebPackaging)
    .dependsOn(apiSharedJVM, foodDataSql, systemDataSql, imageStorageLocal, imageStorageS3, imageProcessorIM, foodSubstRecommender,
               playSecurity, shortUrlServiceClient)
    .settings(commonSettings: _*)


lazy val dataExportService = project.in(file("DataExportService")).enablePlugins(PlayScala, SystemdPlugin, JDebPackaging, ClasspathJarPlugin).dependsOn(apiSharedJVM, foodDataSql, systemDataSql, playSecurity, shortUrlServiceClient)

lazy val shortUrlServiceApi = project.in(file("ShortUrlServiceAPI"))

lazy val shortUrlServiceClient = project.in(file("ShortUrlServiceClient")).dependsOn(shortUrlServiceApi)

lazy val shortUrlService = project.in(file("ShortUrlService")).enablePlugins(PlayScala, SystemdPlugin, JDebPackaging, ClasspathJarPlugin).dependsOn(apiSharedJVM, systemDataSql).dependsOn(shortUrlServiceApi, playUtils)

onLoad.in(Global) ~= { f => s => resolveInternalDependenciesImpl(f(s)) }
