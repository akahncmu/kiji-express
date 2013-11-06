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
package org.kiji.express.util

import scala.annotation.implicitNotFound
import org.kiji.express.Cell

private[express] object CellMathUtil {


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
  def sum[T](slice: Stream[Cell[T]])(implicit num: Numeric[T]): T = {
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
  def mean[T](slice: Stream[Cell[T]])(implicit num: Numeric[T]): Double = {
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

  def min[T](slice: Stream[Cell[T]])(implicit cmp: Ordering[T]): T = {
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

  def max[T](slice: Stream[Cell[T]])(implicit cmp: Ordering[T]): T = {
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
  def stddev[T](slice:Stream[Cell[T]])(implicit num: Numeric[T]): Double = {
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
  def sumSquares[T](slice: Stream[Cell[T]])(implicit num: Numeric[T]): T = {
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
  def variance[T](slice: Stream[Cell[T]])(implicit num: Numeric[T]): Double = {
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
