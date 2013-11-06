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

import java.lang.{Long => JLong}
import java.util.{Map => JMap}

import scala.collection.JavaConverters.asScalaIteratorConverter

import scala.annotation.implicitNotFound
import scala.math.Numeric

import org.kiji.annotations.Inheritance
import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.MapFamilyVersionIterator
import org.kiji.schema.ColumnVersionIterator
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiRowData
import scala.collection.JavaConversions

@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
trait KijiSlice[T] extends Iterable[Cell[T]] with java.io.Serializable

/**
 * A factory for KijiSlices.
 */
object KijiSlice {

  implicit def iterableToKijiSlice[T](itr:Iterator[Cell[T]]) : KijiSlice[T] = KijiSlice(itr)
  implicit def seqToKijiSlice[T](sq:Seq[Cell[T]]) : KijiSlice[T] = KijiSlice(sq)

  private[express] def apply[T](cells: Seq[Cell[T]]): SeqKijiSlice[T] = {
    new SeqKijiSlice[T](cells)
  }

  private[express] def apply[T](cells: Iterator[Cell[T]]): PagedKijiSlice[T] = {
    new PagedKijiSlice[T](cells)
  }

  /**
   * Creates a slice using the cells for a family in a row data.
   *
   * @param row containing cells for the slice.
   * @param family whose cells should be used for the slice.
   * @tparam T the type of data in the cells.
   * @return a new slice of cells in the specified family.
   */
  private[express] def apply[T](row: KijiRowData, family: String): SeqKijiSlice[T] = {
    KijiSlice(row.asIterable[T](family).iterator().asScala.toSeq.map(Cell.apply[T]))
  }

  /**
   * Creates a slice using the cells for a column in a row data.
   *
   * @param row containing cells for the slice.
   * @param family of the column whose cells should be used for the slice.
   * @param qualifier of the column whose cells should be used for the slice.
   * @tparam T the type of data in the cells.
   * @return a new slice of cells in the specified family.
   */
  private[express] def apply[T](row: KijiRowData,
      family: String,
      qualifier: String): SeqKijiSlice[T] = {
    KijiSlice(row.asIterable[T](family, qualifier).iterator().asScala.toSeq.map(Cell.apply[T]))  }

  private[express] def apply[T](family: String, qualifier: String,
                                columnIterator: ColumnVersionIterator[T]): PagedKijiSlice[T] = {
    val itr: Iterator[Cell[T]] = JavaConversions.asScalaIterator(columnIterator).map { entry: JMap.Entry[JLong, T] =>
      new Cell(family, qualifier, entry.getKey, entry.getValue)
    }
    apply(itr)
  }

  private[express] def apply[T](family: String, mapFamilyIterator: MapFamilyVersionIterator[T]): PagedKijiSlice[T] =  {
    val itr = JavaConversions.asScalaIterator(mapFamilyIterator).map { entry: MapFamilyVersionIterator.Entry[T] =>
      new Cell[T](family, entry.getQualifier, entry.getTimestamp, entry.getValue)
    }
    apply(itr)
  }

  /**
   * Returns the sum of values, specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @tparam T type of the underlying cells in the slice
   * @param slice the KijiSlice to operate on
   * @return the sum of values, specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
    + "accepts a function argument.")
  def sum[T](slice: KijiSlice[T])(implicit num: Numeric[T]): T = {
    slice.foldLeft(num.zero) { (sum: T, cell: Cell[T]) =>
      num.plus(sum, cell.datum)
    }
  }

  /**
   * Returns the average value, assuming the value is numeric,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @tparam T type of the underlying cells in the slice
   * @param slice the KijiSlice to be operated on
   * @return the average value, assuming the value is numeric,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
    + "accepts a function argument.")
  def mean[T](slice: KijiSlice[T])(implicit num: Numeric[T]): Double = {
    val n = slice.size
    slice.foldLeft(0.0) { (mean: Double, cell: Cell[T]) =>
      mean + (num.toDouble(cell.datum) / n)
    }

  }

  /**
   * Returns the minimum value,
   * of the KijiSlice's underlying [[org.kiji.express.Cell]]s assuming the Cell's value is numeric.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @tparam T the type of underlying cells in the slice
   * @return the minimum value, specified by the retrieval function,
   *    from the [[org.kiji.express.Cell]]s of the underlying the KijiSlice
   */

  def min[T](slice: KijiSlice[T])(implicit cmp: Ordering[T]): T = {
    slice.min(Ordering.by { cell: Cell[T] => cell.datum }).datum
  }

  /**
   * Returns the minimum value,
   * of the KijiSlice's underlying [[org.kiji.express.Cell]]s assuming the Cell's value is numeric.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @tparam T the type of underlying cells in the slice
   * @return the minimum value, specified by the retrieval function,
   *    from the [[org.kiji.express.Cell]]s of the underlying the KijiSlice
   */

  def max[T](slice: KijiSlice[T])(implicit cmp: Ordering[T]): T = {
    slice.max(Ordering.by { cell: Cell[T] => cell.datum }).datum
  }

  /**
   * Returns the standard deviation, where each value is specified by the retrieval function,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * @tparam T type of the underlying cells in the Slice
   * @return the standard deviation, where each value is specified by the retrieval function,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  def stddev[T](slice: KijiSlice[T])(implicit num: Numeric[T]): Double = {
    scala.math.sqrt(variance(slice))
  }

  /**
   * Returns the squared sum of values, assuming the value is numeric,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * @return the squared sum of values, assuming the value is numeric,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
    + "accepts a function argument.")
  def sumSquares[T](slice: KijiSlice[T])(implicit num: Numeric[T]): T = {
    slice.foldLeft(num.zero) { (sumSquares: T, cell: Cell[T]) =>
      num.plus(sumSquares, num.times(cell.datum, cell.datum))
    }
  }
  /**
   * Returns the variance, assuming each cell's value is numeric,
   * of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   *
   * <b>Note:</b> This method will not compile unless the cells contained within
   * this KijiSlice contain numeric values.
   *
   * This method uses a numerically stable algorithm for computing variance that prevents overflow for very large values
   *
   * @return the variance, assuming each cell's value is numeric,
   *    of the underlying KijiSlice's [[org.kiji.express.Cell]]s.
   */
  @implicitNotFound("KijiSlice cells not numeric. Please use the method that "
    + "accepts a function argument.")
  def variance[T](slice: KijiSlice[T])(implicit num: Numeric[T]): Double = {
    val (n, mean, m2) = slice.foldLeft((0L, 0.0, 0.0)) { (acc: (Long, Double, Double), cell: Cell[T]) =>
      val (n, mean, m2) = acc
      println("cell: " + cell.datum)
      println("acc: " + acc)
      val x = num.toDouble(cell.datum)
    
      val nPrime = n + 1
      val delta = x - mean
      val meanPrime = mean + delta / nPrime
      val m2Prime = m2 + delta * (x - meanPrime)
    
      (nPrime, meanPrime, m2Prime)
    }
    m2 / n
  }

}
 */
