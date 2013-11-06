

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
 *
package org.kiji.express

import org.kiji.annotations.{Inheritance, ApiStability, ApiAudience}
/**
 * A collection of [[org.kiji.express.Cell]]s that can be grouped and ordered as needed. Cells are
 * the smallest unit of information in a Kiji table; each cell contains a datum (as well as column
 * family, qualifier, and version.) Slices are initially ordered first by qualifier and then reverse
 * chronologically (latest first) by version.
 *
 * ===Ordering===
 * The order of the underlying cells can be modified arbitrarily, but we provide convenience methods
 * for common use cases.
 *
 * To order a KijiSlice chronologically, you may write {{{
 * chronological: KijiSlice = slice.orderChronologically()
 * }}}.
 * To order a KijiSlice reverse chronologically, you may write {{{
 * reverseChronological: KijiSlice =slice.orderReverseChronologically()
 * }}}.
 * To order a KijiSlice by column qualifier, you may write {{{
 * qualifier: KijiSlice = slice.orderByQualifier()
 * }}}.
 *
 *
 * ===Grouping===
 * KijiSlices can be grouped together by arbitrary criteria, but we provide a convenience method for
 * a common case.
 *
 * To group KijiSlices by column qualifier, you may write
 * {{{
 * groupedSlices: Map[String, KijiSlice[T]] = slice.groupByQualifier()
 * }}}.
 *
 * Slices can also be arbitrarily grouped by passing in a discriminator function, that defines the
 * grouping criteria, to groupBy().
 *
 * Accessing Values:
 * The underlying collection of cells can be obtained by {{{
 * myCells: Seq[Cell] = mySlice.cells
 * }}}.
 * This Sequence will respect the ordering of the KijiSlice.
 *
 *
 * @param cells A sequence of [[org.kiji.express.Cell]]s for a single entity.
 * @tparam T is the type of the data stored in the underlying cells.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
class SeqKijiSlice[T] private[express] (val cells: Seq[Cell[T]]) extends KijiSlice[T]
with Seq[Cell[T]]{
  override def iterator: Iterator[Cell[T]] = cells.iterator

  /**
   * Gets the number of underlying [[org.kiji.express.Cell]]s.
   *
   * @return the number of underlying [[org.kiji.express.Cell]]s.
   */

  override val length: Int = cells.length

  override def equals(otherSlice: Any): Boolean = otherSlice match {
    case otherSlice: SeqKijiSlice[_] => (otherSlice.cells == cells)
    case _ => false
  }

  override def hashCode(): Int = {
    cells.hashCode()
  }

  override def toString(): String = {
    "KijiSlice: %s".format(cells.toString)
  }

  def apply(idx: Int): Cell[T] = cells(idx)

}

object SeqKijiSlice {
  implicit def seqToSeqKijiSlice[T](seq: Seq[Cell[T]]): SeqKijiSlice[T] = KijiSlice(seq)

}
 */