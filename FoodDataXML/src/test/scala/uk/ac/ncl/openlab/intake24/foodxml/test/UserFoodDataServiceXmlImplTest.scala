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

package uk.ac.ncl.openlab.intake24.foodxml.test

import com.google.inject.Inject
import com.google.inject.Singleton
import uk.ac.ncl.openlab.intake24.foodxml.UserFoodDataServiceXmlImpl
import uk.ac.ncl.openlab.intake24.services.UserFoodDataServiceTest
import uk.ac.ncl.openlab.intake24.foodxml.XmlDataSource

class UserFoodDataServiceXmlImplTest extends UserFoodDataServiceTest {
  val service = new UserFoodDataServiceXmlImpl(new XmlDataSource(getClass.getResource("/test1").toURI().getPath()))
}