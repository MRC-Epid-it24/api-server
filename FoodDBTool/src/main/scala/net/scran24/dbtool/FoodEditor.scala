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

This file is based on Intake24 v1.0.

© Crown copyright, 2012, 2013, 2014

Licensed under the Open Government Licence 3.0: 

http://www.nationalarchives.gov.uk/doc/open-government-licence/
*/

package net.scran24.dbtool

import javax.swing.JPanel
import javax.swing.JSplitPane
import javax.swing.JLabel


import uk.ac.ncl.openlab.intake24.InheritableAttributes

import uk.ac.ncl.openlab.intake24.AsServedSet
import uk.ac.ncl.openlab.intake24.GuideImage
import uk.ac.ncl.openlab.intake24.DrinkwareSet
import javax.swing.JScrollPane
import javax.swing.JOptionPane
import javax.swing.JButton
import java.awt.FlowLayout
import java.awt.Dimension
import java.awt.BorderLayout
import java.awt.event.ActionListener
import java.awt.event.ActionEvent
import Util._
import SwingUtil._
import java.io.File
import java.util.UUID
import uk.ac.ncl.openlab.intake24.foodxml.FoodDef
import uk.ac.ncl.openlab.intake24.foodxml.CategoryDef
import uk.ac.ncl.openlab.intake24.FoodRecord
import uk.ac.ncl.openlab.intake24.MainFoodRecord
import uk.ac.ncl.openlab.intake24.LocalFoodRecord
import uk.ac.ncl.openlab.intake24.foodxml.XmlCategoryRecord
import uk.ac.ncl.openlab.intake24.foodxml.XmlFoodGroup
import uk.ac.ncl.openlab.intake24.foodxml.XmlFoodRecord

case class SearchFoodWrapper(food: XmlFoodRecord) {
  override def toString() = food.description + " (" + food.code + ")"
}

case class SearchCategoryWrapper(category: XmlCategoryRecord) {
  override def toString() = category.description + " (" + category.code + ")"
}

class FoodEditor(foods: Seq[XmlFoodRecord], foodGroups: Seq[XmlFoodGroup], categories: Seq[XmlCategoryRecord], portionResources: PortionResources, imageDirectory: ImageDirectory) extends JPanel {
  val blayout = new BorderLayout()
  setLayout(blayout)

  val mutableFoods = new MutableFoods(foods)
  val mutableCategories = new MutableCategories(categories)
  val portionSizeResolver = new PortionSizeResolver(mutableFoods, mutableCategories)

  val foodTree = new FoodTree(mutableFoods, mutableCategories, portionSizeResolver, select, confirmSelectionChange)

  var editor: Option[DefEditor] = None

  var changed = false

  def addNewFood(food: XmlFoodRecord) = {
     foodTree.selectedCategory match {
      case Some(code) => {
        val cat = mutableCategories.find(code).get
        mutableCategories.update(code, cat.copy(foods = cat.foods :+ food.code))
        foodTree.categoryUpdated(code)
      }
      case None => {}
    }

    mutableFoods.create(food)
    foodTree.foodAdded(food)

    changed = true
  }
  
  def newFood = addNewFood(XmlFoodRecord(mutableFoods.tempcode, "New food", 0, InheritableAttributes(None, None, None), Map("NDNS" -> "-1"), Seq()))
  def cloneFood(source: XmlFoodRecord) = addNewFood(source.copy(code = mutableFoods.tempcode(), description = "Copy of " + source.description))

  def newCategory = {
    val tempCategory = XmlCategoryRecord(mutableCategories.tempcode, "New category", Seq(), Seq(), false, InheritableAttributes(None, None, None), Seq())

    foodTree.selectedCategory match {
      case Some(code) => {
        val cat = mutableCategories.find(code).get
        mutableCategories.update(code, cat.copy(subcategories = cat.subcategories :+ tempCategory.code))
      }
      case None => {}
    }

    mutableCategories.create(tempCategory)
    foodTree.categoryAdded(tempCategory)

    changed = true
  }

  def updateFood(code: String, food: XmlFoodRecord, categories: Seq[XmlCategoryRecord]): Boolean = {
    val existing = mutableFoods.find(food.code)
    val portionSizeProblems = food.portionSizeMethods.flatMap(checkPortionSize)
    if (existing.isDefined && code != food.code) {
      JOptionPane.showMessageDialog(this, "<html>Food code <strong>" + code + "</strong> is already used by <strong>" + existing.get.description + "</strong></html>", "Cannot accept changes", JOptionPane.ERROR_MESSAGE)
      false
    } else if (!portionSizeProblems.isEmpty) {
      JOptionPane.showMessageDialog(this, "<html><p>" + portionSizeProblems.mkString("</p><p>") + "</p</html>", "Cannot accept changes", JOptionPane.ERROR_MESSAGE)
      false
    } else {
      deleteFood(code)

      categories.foreach(cat => {
        val code = cat.code
        val oldCat = mutableCategories.find(code).get
        mutableCategories.update(code, oldCat.copy(foods = (oldCat.foods :+ food.code)))
        foodTree.categoryUpdated(code)
      })

      mutableFoods.create(food)
      foodTree.foodAdded(food)

      true
    }
  }

  def deleteFood(code: String) = {
    val categories = mutableCategories.foodSuperCategories(code)

    categories.foreach(cat => {
      mutableCategories.update(cat.code, cat.copy(foods = cat.foods.filterNot(_ == code)))
      foodTree.categoryUpdated(cat.code)
    })

    mutableFoods.delete(code)
    foodTree.entryDeleted(code)

    changed = true
  }

  def deleteCategory(code: String, triggerAddition: Boolean) = {
    val category = mutableCategories.find(code).get

    val superCategories = mutableCategories.categorySuperCategories(code)

    superCategories.foreach(cat => {
      mutableCategories.update(cat.code, cat.copy(subcategories = cat.subcategories.filterNot(_ == code)))
      foodTree.categoryUpdated(cat.code)
    })

    foodTree.entryDeleted(code)

    if (triggerAddition) {
      category.foods.foreach(code =>
        if (mutableCategories.foodSuperCategories(code).isEmpty)
          foodTree.foodAdded(mutableFoods.find(code).get) // trigger addition to uncategorised foods node
          )

      category.subcategories.foreach(code =>
        if (mutableCategories.categorySuperCategories(code).isEmpty)
          foodTree.categoryAdded(mutableCategories.find(code).get)) // trigger addition as root categories
    }

    mutableCategories.delete(code)

    changed = true
  }

  def deleteFoodRequest(food: XmlFoodRecord) = {
    if (JOptionPane.showConfirmDialog(this, "Are you sure you want to delete the food  \"" + food.description + "\" (" + food.code + ")?", "Delete food", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
      deleteFood(food.code)
    }
  }

  def updateCategory(code: String, category: XmlCategoryRecord, categories: Seq[XmlCategoryRecord]): Boolean = {
    val existing = mutableCategories.find(category.code)
    val portionSizeProblems = category.portionSizeMethods.flatMap(checkPortionSize)
    if (existing.isDefined && code != category.code) {
      JOptionPane.showMessageDialog(this, "<html>Intake24 code <strong>" + code + "</strong> is already used by <strong>" + existing.get.description + "</strong></html>", "Cannot accept changes", JOptionPane.ERROR_MESSAGE)
      false
    } else if (!portionSizeProblems.isEmpty) {
      JOptionPane.showMessageDialog(this, "<html><p>" + portionSizeProblems.mkString("</p><p>") + "</p</html>", "Cannot accept changes", JOptionPane.ERROR_MESSAGE)
      false
    } else {
      deleteCategory(code, false)

      categories.foreach(cat => {
        val code = cat.code
        val oldCat = mutableCategories.find(code).get
        mutableCategories.update(code, oldCat.copy(subcategories = (oldCat.subcategories :+ category.code)))
        foodTree.categoryUpdated(code)
      })

      mutableCategories.create(category)
      foodTree.categoryAdded(category)

      true
    }
  }

  def deleteCategoryRequest(category: XmlCategoryRecord) = {
    if (JOptionPane.showConfirmDialog(this, "Are you sure you want to delete the category  \"" + category.description + "\" (" + category.code + ")?", "Delete category", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
      deleteCategory(category.code, true)
    }
  }

  def confirmSelectionChange() = editor match {
    case Some(e) if (e.changed) => {
      JOptionPane.showConfirmDialog(this, "Do you want to save changes you have made to \"" + e.description + "\" ?", "Confirm changes", JOptionPane.YES_NO_CANCEL_OPTION) match {
        case JOptionPane.YES_OPTION => { e.acceptChanges(); true }
        case JOptionPane.NO_OPTION => true
        case JOptionPane.CANCEL_OPTION => false
      }
    }
    case _ => true
  }

  def select(node: Option[FoodTreeNode]): Unit = {
    remove(blayout.getLayoutComponent(BorderLayout.CENTER))
    editor = None

    cloneFoodButton.setEnabled(false)
    cloneFoodButton.setToolTipText("Select a food to enable this button")
    
    node match {
      case Some(FoodWrapper(food)) => {
        val e = new FoodDefPanel(portionSizeResolver, mutableFoods, foodGroups, mutableCategories, portionResources, imageDirectory, food, updateFood, deleteFoodRequest)
        val scroll2 = new JScrollPane(e)
        scroll2.getVerticalScrollBar().setUnitIncrement(12)
        add(scroll2, BorderLayout.CENTER)
        editor = Some(e)
        cloneFoodButton.setEnabled(true)
        cloneFoodButton.setToolTipText(null);
      }
      case Some(CategoryWrapper(category)) => {
        val e = new CategoryDefPanel(portionSizeResolver, mutableFoods, mutableCategories, portionResources, imageDirectory, category, updateCategory, deleteCategoryRequest)
        val scroll2 = new JScrollPane(e)
        scroll2.getVerticalScrollBar().setUnitIncrement(12)
        add(scroll2, BorderLayout.CENTER)
        editor = Some(e)
      }
      case _ => add(new JLabel("Nothing selected"), BorderLayout.CENTER)
    }

    revalidate()
  }

  def areChangesMade() = changed || editor.map(_.changed).getOrElse(false)

  def acceptChanges(): Boolean = editor match {
    case Some(editor) => editor.acceptChanges()
    case None => true
  }

  val buttons = new JPanel(new FlowLayout(FlowLayout.LEADING))
  val newFoodButton = new JButton("New food")
  newFoodButton.addActionListener(new ActionListener() {
    override def actionPerformed(e: ActionEvent) = newFood
  })

  val cloneFoodButton = new JButton("Clone food")
  cloneFoodButton.setEnabled(false)
  cloneFoodButton.addActionListener(new ActionListener() {
    override def actionPerformed(e: ActionEvent) = cloneFood(foodTree.selectedFood.get)
  })

  val newCatButton = new JButton("New category")
  newCatButton.addActionListener(new ActionListener() {
    override def actionPerformed(e: ActionEvent) = newCategory
  })

  val findButton = new JButton("Find food")
  findButton.addActionListener(() => {

    val dialog = new SelectionDialog[SearchFoodWrapper](ownerFrame(this), "Select food", mutableFoods.snapshot().map(SearchFoodWrapper(_)))
    dialog.setVisible(true)
    dialog.choice.foreach(food => foodTree.selectEntry(food.food.code))
  })

  val findCatButton = new JButton("Find category")
  findCatButton.addActionListener(() => {
    val dialog = new SelectionDialog[SearchCategoryWrapper](ownerFrame(this), "Select category", mutableCategories.snapshot().map(SearchCategoryWrapper(_)))
    dialog.setVisible(true)
    dialog.choice.foreach(w => foodTree.selectEntry(w.category.code))
  })

  buttons.add(newFoodButton)
  buttons.add(cloneFoodButton)
  buttons.add(newCatButton)
  buttons.add(findButton)
  buttons.add(findCatButton)

  val scroll = new JScrollPane()
  scroll.setViewportView(foodTree)
  scroll.getVerticalScrollBar().setUnitIncrement(12)
  scroll.setPreferredSize(new Dimension(400, 0))

  add(buttons, BorderLayout.NORTH)
  add(scroll, BorderLayout.WEST)
  add(new JLabel("Nothing selected"), BorderLayout.CENTER)
}