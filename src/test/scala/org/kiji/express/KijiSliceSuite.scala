/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express

import org.junit.runner.RunWith
import org.kiji.express.util.CellMathUtil
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class KijiSliceSuite extends FunSuite with ShouldMatchers {
  val cell0 = Cell[Long]("info", "number", 0L, 0L)
  val cell1 = Cell[Long]("info", "number", 1L, 1L)
  val cell2 = Cell[Long]("info", "number", 2L, 2L)
  val cell3 = Cell[Long]("info", "number", 3L, 3L)
  val cell4 = Cell[Long]("info", "number", 4L, 4L)
  val cell5 = Cell[Long]("info", "number", 5L, 4L)
  val cell6 = Cell[Long]("info", "number", 6L, 6L)
  val cell7 = Cell[Long]("info", "number", 7L, 7L)
  val cellSeq : List[Cell[Long]] = List[Cell[Long]](cell7, cell6, cell5, cell4, cell3, cell2, cell1,
      cell0)

  val mapCell0 = Cell[Long]("info", "a", 0L, 0L)
  val mapCell1 = Cell[Long]("info", "a", 1L, 1L)
  val mapCell2 = Cell[Long]("info", "b", 2L, 0L)
  val mapCell3 = Cell[Long]("info", "b", 3L, 3L)
  val mapCell4 = Cell[Long]("info", "c", 4L, 0L)
  val mapCellSeq : List[Cell[Long]] = List[Cell[Long]](mapCell4, mapCell3, mapCell2, mapCell1,
      mapCell0)

  test("KijiSlice can be instantiated and maintain order.") {
    val slice: Stream[Cell[Long]] =  cellSeq.toStream
    assert(slice.head == cell7)
    assert(slice.last == cell0)
    assert(slice == cellSeq.toStream)
  }

  test("KijiSlice can order by time.") {
    val slice: Stream[Cell[Long]] =  cellSeq.toStream
    // Reverse the ordering.
    val reversedSlice: Stream[Cell[Long]] = slice.sortBy(_.version)
    assert(reversedSlice.head == cell0)
    assert(reversedSlice.last == cell7)

    val reReversedSlice: Stream[Cell[Long]] = reversedSlice.sortBy(-_.version)
    assert(reReversedSlice.head == cell7)
    assert(reReversedSlice.last == cell0)
    assert(reReversedSlice == cellSeq)
  }

  test("KijiSlice can order by qualifier.") {
    val slice: Stream[Cell[Long]]  =  mapCellSeq.toStream
    // Order alphabetically, by qualifier.
    val alphabeticalSlice: Stream[Cell[Long]]  = slice.sortBy(_.qualifier)
    assert(alphabeticalSlice.head == mapCell1)
    assert(alphabeticalSlice.last == mapCell4)
  }

  test("KijiSlice can groupBy qualifier.") {
    val slice: Stream[Cell[Long]] =  mapCellSeq.toStream
    // Group by qualifiers.
    val groupedSlices = slice.groupBy(_.qualifier)
    assert(3 == groupedSlices.size)
    val sliceA = groupedSlices.get("a").get
    assert(2 == sliceA.size)
    val sliceB = groupedSlices.get("b").get
    assert(2 == sliceB.size)
    val sliceC = groupedSlices.get("c").get
    assert(1 == sliceC.size)
  }

  test("KijiSlice can groupBy datum.") {
    val slice: Stream[Cell[Long]]  = mapCellSeq.toStream
    // Group by the datum contained in the cell.
    val groupedSlices = slice.groupBy(_.datum)
    assert(3 == groupedSlices.size)
    val slice0 = groupedSlices.get(0L).get
    assert(3 == slice0.size)
    assert(4L == slice0.head.version)
    val slice1 = groupedSlices.get(1L).get
    assert(1 == slice1.size)
    val slice3 = groupedSlices.get(3L).get
    assert(1 == slice3.size)
  }

  test("KijiSlice should properly sum simple types")
  {
    val slice: Stream[Cell[Long]]  =  mapCellSeq.toStream
    assert(4 == CellMathUtil.sum(slice))
  }

  test("KijiSlice should properly compute the squared sum of simple types")
  {
    val slice: Stream[Cell[Long]]  =  mapCellSeq.toStream
    assert(10.0 == CellMathUtil.sumSquares(slice))
  }

  test("KijiSlice should properly compute the average of simple types")
  {
    val slice:Stream[Cell[Long]] =  mapCellSeq.toStream
   assert(.8 == CellMathUtil.mean(slice))
  }

  test("KijiSlice should properly compute the standard deviation of simple types")
  {
    val slice: Stream[Cell[Long]] =  mapCellSeq.toStream
    CellMathUtil.stddev(slice) should be (1.16619 plusOrMinus 0.1)
  }

  test("KijiSlice should properly compute the variance of simple types")
  {
    val slice: Stream[Cell[Long]] = mapCellSeq.toStream
    CellMathUtil.variance(slice) should be (1.36 plusOrMinus 0.1)
  }

  test("KijiSlice should properly find the minimum of simple types")
  {
    val slice: Stream[Cell[Long]] = mapCellSeq.toStream
    assert(0.0 == CellMathUtil.min(slice))
  }

  test("KijiSlice should properly find the maximum of simple types")
  {
    val slice: Stream[Cell[Long]] = mapCellSeq.toStream
    assert(3.0 == CellMathUtil.max(slice))
  }
}
