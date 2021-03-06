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

package uk.ac.ncl.openlab.intake24.foodxml

case class Foods(foods: Seq[XmlFoodRecord]) {
  val foodMap = foods.map(c => (c.code, c)).toMap

  def find(code: String) = foodMap.get(code) match {
    case Some(food) => food
    case None => throw new IllegalArgumentException(s"food with code $code is undefined")
  }

  def findOption(code: String) = foodMap.get(code)
}
