/*
This file is part of Intake24.

© Crown copyright, 2012, 2013, 2014.

This software is licensed under the Open Government Licence 3.0:

http://www.nationalarchives.gov.uk/doc/open-government-licence/
*/

package net.scran24.dbtool

import net.scran24.fooddef.GuideImage
import net.scran24.fooddef.AsServedSet
import net.scran24.fooddef.DrinkwareSet

case class PortionResources (asServedSets: Seq[AsServedSet], guideImages: Seq[GuideImage], drinkwareSets: Seq[DrinkwareSet])