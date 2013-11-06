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

package org.kiji.express.flow

import org.junit.runner.RunWith
import org.apache.hadoop.hbase.HConstants
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.express.Cell
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.filter.RegexQualifierColumnFilter

@RunWith(classOf[JUnitRunner])
class FlowModuleSuite extends FunSuite {
  val tableURI = "kiji://.env/default/table"

  test("Flow module forbids creating an input map-type column with a qualifier in the column "
      + "name.") {
    intercept[KijiInvalidNameException] {
      val colReq = new ColumnFamilyRequestInput("info:word")
    }
  }

  test("Flow module forbids creating an output map-type column with a qualifier in the column "
      + "name.") {
    intercept[KijiInvalidNameException] {
      val colReq = new ColumnFamilyRequestOutput("info:word", "foo")
    }
  }

  test("Flow module permits creating an output map-type column specifying the qualifier field") {
    val colReq = new ColumnFamilyRequestOutput("searches", "terms")
  }

  test("Flow module permits specifying a qualifier regex on map-type columns requests.") {
    val colReq = new ColumnFamilyRequestInput(
        "search",
        filter = Some(new RegexQualifierColumnFilter(""".*\.com"""))
    )

    // TODO: Test it filters keyvalues correctly.
    assert(colReq.filter.get.isInstanceOf[RegexQualifierColumnFilter])
  }

  test("Flow module permits specifying a qualifier regex (with filter) on map-type column "
      + "requests.") {
    val colReq = new ColumnFamilyRequestInput("search",
      filter=Some(new RegexQualifierColumnFilter(""".*\.com""")))

    // TODO: Test it filters keyvalues correctly.
    assert(colReq.filter.get.isInstanceOf[RegexQualifierColumnFilter])
  }

  test("Flow module permits specifying versions on map-type columns without qualifier regex.") {
    val colReq = ColumnFamilyRequestInput("search", maxVersions=2)
    assert(2 === colReq.maxVersions)
  }

  test("Flow module permits specifying versions on a group-type column.") {
    val colReq = QualifiedColumnRequestInput("info", "word", maxVersions=2)
    assert(2 === colReq.maxVersions)
  }

  test("Flow module uses default versions of 1 for map-type and group-type column requests.") {
    val groupReq = QualifiedColumnRequestInput("info", "word")
    val mapReq = ColumnFamilyRequestInput("searches")

    assert(1 === groupReq.maxVersions)
    assert(1 === mapReq.maxVersions)
  }

  test("Flow module permits creating inputs and outputs with no mappings.") {
    val input: KijiSource = KijiInput(tableURI, columns = Map[ColumnRequestInput, Symbol]())
    val output: KijiSource = KijiOutput(tableURI, columns = Map[Symbol, ColumnRequestOutput]())

    assert(input.inputColumns.isEmpty)
    assert(input.outputColumns.isEmpty)
    assert(output.inputColumns.isEmpty)
    assert(output.outputColumns.isEmpty)
  }

  test("Flow module permits creating KijiSources as inputs with default options.") {
    val input: KijiSource = KijiInput(tableURI, "info:word" -> 'word)
    val expectedScheme = new KijiScheme(
        timeRange = All,
        timestampField = None,
        loggingInterval = 1000,
        inputColumns = Map("word" -> QualifiedColumnRequestInput("info", "word")))

    assert(expectedScheme === input.hdfsScheme)
  }

  test("Flow module permits specifying timerange for KijiInput.") {
    val input = KijiInput(tableURI, timeRange=Between(0L,40L), columns="info:word" -> 'word)
    val expectedScheme = new KijiScheme(
        Between(0L, 40L),
        None,
        1000,
        Map("word" -> QualifiedColumnRequestInput("info", "word")))

    assert(expectedScheme === input.hdfsScheme)
  }

  test("Flow module permits creating KijiSources with multiple columns.") {
    val input: KijiSource = KijiInput(tableURI, "info:word" -> 'word, "info:title" -> 'title)
    val expectedScheme: KijiScheme = {
      new KijiScheme(
          All,
          None,
          1000,
          Map(
              "word" -> QualifiedColumnRequestInput("info", "word"),
              "title" -> QualifiedColumnRequestInput("info", "title")))
    }

    assert(expectedScheme === input.hdfsScheme)
  }

  test("Flow module permits specifying options for a column.") {
    val input: KijiSource =
        KijiInput(tableURI, Map(QualifiedColumnRequestInput("info", "word") -> 'word))

    val input2: KijiSource =
        KijiInput(
            tableURI,
            Map(QualifiedColumnRequestInput("info", "word", maxVersions = 1) -> 'word))

    val input3: KijiSource = KijiInput(tableURI, Map( new ColumnFamilyRequestInput(
        "searches",
        maxVersions = 1,
        filter = Some(new RegexQualifierColumnFilter(".*"))) -> 'word))
  }

  test("A qualified Column can specify a replacement that is a single value.") {
    val defaultVal = "replacement"
    val col = QualifiedColumnRequestInput(
        "family", "qualifier",
        default = Some(List(Cell(
            "family",
            "qualifier",
            HConstants.LATEST_TIMESTAMP,
            defaultVal)).toStream))
    assert(col.isInstanceOf[QualifiedColumnRequestInput])

    val qualifiedColumn = col.asInstanceOf[QualifiedColumnRequestInput]
    val replacementOption: Option[Stream[Cell[_]]] = qualifiedColumn.default
    assert(replacementOption.isDefined)

    val replacement = replacementOption.get

    assert(1 === replacement.size)
    assert(defaultVal === replacement.head.datum)
  }

  test("A ColumnFamily can specify a replacement that is a single value.") {
    val col = new ColumnFamilyRequestInput("family")
    .replaceMissingWith("qualifier", "replacement")
    assert(col.isInstanceOf[ColumnFamilyRequestInput])

    val columnFamily = col.asInstanceOf[ColumnFamilyRequestInput]
    val replacementOption: Option[Stream[Cell[_]]] = columnFamily.default
    assert(replacementOption.isDefined)

    val replacement = replacementOption.get

    assert(1 === replacement.size)
    assert("replacement" === replacement.head.datum)
  }

  test("A qualified Column can specify a replacement that is a single value with a timestamp.") {
    val defaultSlice = List(Cell("family", "qualifier", 10L, "replacement")).toStream
    val col = QualifiedColumnRequestInput("family", "qualifier", default = Some(defaultSlice))
    assert(col.isInstanceOf[QualifiedColumnRequestInput])

    val qualifiedColumn = col.asInstanceOf[QualifiedColumnRequestInput]
    val replacementOption: Option[Stream[Cell[_]]] = qualifiedColumn.default
    assert(replacementOption.isDefined)

    val replacement = replacementOption.get

    assert(1 === replacement.size)
    assert("replacement" === replacement.head.datum)
    assert(10L === replacement.head.version)
  }

  test("A ColumnFamily can specify a replacement that is a single value with a timestamp.") {
    val defaultSlice = List(Cell("family", "qualifier", 10L, "replacement")).toStream
    val col = new ColumnFamilyRequestInput("family", default = Some(defaultSlice))
    assert(col.isInstanceOf[ColumnFamilyRequestInput])

    val columnFamily = col.asInstanceOf[ColumnFamilyRequestInput]
    val replacementOption: Option[Stream[Cell[_]]] = columnFamily.default
    assert(replacementOption.isDefined)

    val replacement = replacementOption.get

    assert(1 === replacement.size)
    assert("replacement" === replacement.head.datum)
    assert(10L === replacement.head.version)
  }

  test("A qualified Column can specify a replacement that is multiple values.") {
    val defaultSlice =  List(
        Cell("family", "qualifier", HConstants.LATEST_TIMESTAMP, "replacement1"),
        Cell("family", "qualifier", HConstants.LATEST_TIMESTAMP, "replacement2")).toStream

    val col = QualifiedColumnRequestInput("family", "qualifier", default=Some(defaultSlice))
    assert(col.isInstanceOf[QualifiedColumnRequestInput])

    val qualifiedColumn = col.asInstanceOf[QualifiedColumnRequestInput]
    val replacementOption: Option[Stream[Cell[_]]] = qualifiedColumn.default
    assert(replacementOption.isDefined)

    val replacementData = replacementOption.get

    assert(2 === replacementData.size)
    assert(replacementData.head.datum=="replacement1")
    assert(replacementData.last.datum=="replacement2")
  }

  test("A ColumnFamily can specify a replacement that is multiple values.") {
    val defaultSlice =  List(
        Cell("family", "qualifier1", HConstants.LATEST_TIMESTAMP, "replacement1"),
        Cell("family", "qualifier2", HConstants.LATEST_TIMESTAMP, "replacement2")).toStream

    //val col = new ColumnFamilyRequestInput("family", 'qualifier, default = Some(defaultSlice))
    val col = new ColumnFamilyRequestInput("family", default = Some(defaultSlice))
    assert(col.isInstanceOf[ColumnFamilyRequestInput])

    val columnFamily = col.asInstanceOf[ColumnFamilyRequestInput]
    val replacementOption: Option[Stream[Cell[_]]] = columnFamily.default
    assert(replacementOption.isDefined)

    val replacementData = replacementOption.get.map { c: Cell[_] => (c.qualifier, c.datum) }

    assert(2 === replacementData.size)
    assert(replacementData.contains(("qualifier1", "replacement1")))
    assert(replacementData.contains(("qualifier2", "replacement2")))
  }

  test("A QualifiedColumnRequestInput can specify a replacement that is multiple values with "
      + "timestamps.") {
    val col = QualifiedColumnRequestInput("family", "qualifier")
    assert(col.isInstanceOf[QualifiedColumnRequestInput])

    val qualifiedColumn = col.asInstanceOf[QualifiedColumnRequestInput]
        .replaceMissingWithVersioned(List((10L, "replacement1"), (20L, "replacement2")))
    val replacementOption: Option[Stream[Cell[_]]] = qualifiedColumn.default
    assert(replacementOption.isDefined)

    val replacementData = replacementOption.get.map { c: Cell[_] => (c.version, c.datum) }

    assert(2 === replacementData.size)
    assert(replacementData.contains((10L, "replacement1")))
    assert(replacementData.contains((20L, "replacement2")))
  }

  test("A ColumnFamilyRequestInput can specify a replacement that is multiple values with "
      + "timestamps.") {
    val columnFamily = new ColumnFamilyRequestInput("family")
        .replaceMissingWithVersioned(List(
            ("qualifier1", 10L, "replacement1"),
            ("qualifier2", 20L, "replacement2")))

    val replacementOption: Option[Stream[Cell[_]]] = columnFamily.default
    assert(replacementOption.isDefined)

    val replacementData = replacementOption.get.map { c: Cell[_] =>
      (c.qualifier, c.version, c.datum) }

    assert(2 === replacementData.size)
    assert(replacementData.contains(("qualifier1", 10L, "replacement1")))
    assert(replacementData.contains(("qualifier2", 20L, "replacement2")))
  }

  test("Flow module permits specifying different options for different columns.") {
    val input: KijiSource = KijiInput(
        tableURI,
        Map(
            QualifiedColumnRequestInput("info", "word", maxVersions=1) -> 'word,
            QualifiedColumnRequestInput("info", "title", maxVersions=2) -> 'title))
  }

  test("Flow module permits creating KijiSource with the default timestamp field") {
    val output: KijiSource = KijiOutput(tableURI, 'words -> "info:words")
    val expectedScheme: KijiScheme = new KijiScheme(
        timeRange = All,
        timestampField = None,
        loggingInterval = 1000,
        outputColumns = Map("words" -> QualifiedColumnRequestOutput("info", "words")))
    assert(expectedScheme === output.hdfsScheme)
  }

  test("Flow module permits creating KijiSource with a timestamp field") {
    val output: KijiSource = KijiOutput(tableURI, 'time, 'words -> "info:words")
    val expectedScheme: KijiScheme = new KijiScheme(
        timeRange = All,
        timestampField = Some('time),
        loggingInterval = 1000,
        outputColumns = Map("words" -> QualifiedColumnRequestOutput("info", "words")))
    assert(expectedScheme === output.hdfsScheme)
  }
}
