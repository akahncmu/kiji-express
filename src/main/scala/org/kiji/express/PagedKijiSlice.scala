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

import java.util.{Map => JMap}
import java.lang.{Long => JLong}

import org.kiji.schema.ColumnVersionIterator
import org.kiji.schema.MapFamilyVersionIterator
import org.kiji.annotations.Inheritance
import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * A collection of [[org.kiji.express.Cell]]s, retrieved in pages, that can be seamlessly iterated
 * over. Paging limits the amount of memory used, by limiting the number of cells retrieved at a
 * time. For more information on paging in general, see [[org.kiji.schema.KijiPager]].
 *
 * To use paging in an Express Flow, you specify the maximum number of cells you want in a page.
 * To request no more than 3 cells in memory at a
 * {{{
 *      KijiInput(args("input"))(Map(Column("family:column", all).withPaging(3) -> 'column))

 * }}}
 *
 * If paging is requested for a column in a Kiji table, the tuple field returned will contain a
 * [[org.kiji.express.PagedKijiSlice]].
 *
 * @param cells that can be iterated over for processing.
 * @tparam T is the type of the data contained in the underlying cells.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class PagedKijiSlice[T] private[express] (cells: Iterator[Cell[T]]) extends KijiSlice[T] {
  override def iterator: Iterator[Cell[T]] = cells
}

object PagedKijiSlice {
  implicit def iterableToPagedKijiSlice[T](itr:Iterator[Cell[T]]) : PagedKijiSlice[T] = KijiSlice(itr)
}
 */
