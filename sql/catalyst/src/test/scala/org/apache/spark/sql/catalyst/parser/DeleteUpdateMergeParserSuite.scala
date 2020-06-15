/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, DeleteFromTable, InsertAction, LogicalPlan, MergeIntoTable, Project, SubqueryAlias, UpdateAction, UpdateTable, With}

class DeleteUpdateMergeParserSuite extends AnalysisTest {
  import CatalystSqlParser._

  test("delete from table: delete all") {
    parseCompare("DELETE FROM db.tbl",
      DeleteFromTable(
        UnresolvedRelation(TableIdentifier("tbl", Some("db"))),
        None))
  }

  test("delete from table: with alias and where clause") {
    parseCompare("DELETE FROM db.tbl AS t WHERE t.a = 2",
      DeleteFromTable(
        SubqueryAlias("t", UnresolvedRelation(TableIdentifier("tbl", Some("db")))),
        Some(EqualTo(UnresolvedAttribute("t.a"), Literal(2)))))
  }

  test("delete from table: columns aliases is not allowed") {
    val exc = intercept[ParseException] {
      parsePlan("DELETE FROM db.tbl AS t(a,b,c,d) WHERE d = 2")
    }

    assert(exc.getMessage.contains("Columns aliases are not allowed in DELETE."))
  }

  test("update table: basic") {
    parseCompare(
      """
        |UPDATE db.tbl
        |SET a='Robert', b=32
      """.stripMargin,
      UpdateTable(
        UnresolvedRelation(TableIdentifier("tbl", Some("db"))),
        Seq(Assignment(UnresolvedAttribute("a"), Literal("Robert")),
          Assignment(UnresolvedAttribute("b"), Literal(32))),
        None))
  }

  test("update table: with alias and where clause") {
    parseCompare(
      """
        |UPDATE db.tbl AS t
        |SET t.a='Robert', t.b=32
        |WHERE t.c=2
      """.stripMargin,
      UpdateTable(
        SubqueryAlias("t", UnresolvedRelation(TableIdentifier("tbl", Some("db")))),
        Seq(Assignment(UnresolvedAttribute("t.a"), Literal("Robert")),
          Assignment(UnresolvedAttribute("t.b"), Literal(32))),
        Some(EqualTo(UnresolvedAttribute("t.c"), Literal(2)))))
  }

  test("update table: columns aliases is not allowed") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |UPDATE db.tbl AS t(a,b,c,d)
          |SET b='Robert', c=32
          |WHERE d=2
        """.stripMargin)
    }

    assert(exc.getMessage.contains("Columns aliases are not allowed in UPDATE."))
  }

  test("merge into table: basic") {
    parseCompare(
      """
        |MERGE INTO db1.tbl AS target
        |USING db2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(TableIdentifier("tbl", Some("db1")))),
        SubqueryAlias("source", UnresolvedRelation(TableIdentifier("tbl", Some("db2")))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"),
              UnresolvedAttribute("source.col2"))))),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))),
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))))))
  }

  test("merge into table: using subquery") {
    parseCompare(
      """
        |MERGE INTO db1.tbl AS target
        |USING (SELECT * FROM db2.tbl) AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(TableIdentifier("tbl", Some("db1")))),
        SubqueryAlias("source", Project(Seq(UnresolvedStar(None)),
          UnresolvedRelation(TableIdentifier("tbl", Some("db2"))))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"),
              UnresolvedAttribute("source.col2"))))),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))),
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))))))
  }

  test("merge into table: cte") {
    parseCompare(
      """
        |MERGE INTO db1.tbl AS target
        |USING (WITH s as (SELECT * FROM db2.tbl) SELECT * FROM s) AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(TableIdentifier("tbl", Some("db1")))),
        SubqueryAlias("source", With(Project(Seq(UnresolvedStar(None)),
          UnresolvedRelation(TableIdentifier("s"))),
          Seq("s" -> SubqueryAlias("s", Project(Seq(UnresolvedStar(None)),
            UnresolvedRelation(TableIdentifier("tbl", Some("db2")))))))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"),
              UnresolvedAttribute("source.col2"))))),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))),
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))))))
  }

  test("merge into table: no additional condition") {
    parseCompare(
      """
        |MERGE INTO db1.tbl AS target
        |USING db2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED THEN UPDATE SET target.col2 = source.col2
        |WHEN NOT MATCHED
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(TableIdentifier("tbl", Some("db1")))),
        SubqueryAlias("source", UnresolvedRelation(TableIdentifier("tbl", Some("db2")))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(UpdateAction(None,
          Seq(Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2"))))),
        Seq(InsertAction(None,
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))))))
  }

  test("merge into table: star") {
    parseCompare(
      """
        |MERGE INTO db1.tbl AS target
        |USING db2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET *
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT *
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(TableIdentifier("tbl", Some("db1")))),
        SubqueryAlias("source", UnresolvedRelation(TableIdentifier("tbl", Some("db2")))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq())),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))),
          Seq()))))
  }

  test("merge into table: columns aliases are not allowed") {
    Seq("target(c1, c2)" -> "source", "target" -> "source(c1, c2)").foreach {
      case (targetAlias, sourceAlias) =>
        val exc = intercept[ParseException] {
          parsePlan(
            s"""
               |MERGE INTO db1.tbl AS $targetAlias
               |USING db2.tbl AS $sourceAlias
               |ON target.col1 = source.col1
               |WHEN MATCHED AND (target.col2='delete') THEN DELETE
               |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
               |WHEN NOT MATCHED AND (target.col2='insert')
               |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
            """.stripMargin)
        }

        assert(exc.getMessage.contains("Columns aliases are not allowed in MERGE."))
    }
  }

  test("merge into table: at most two matched clauses") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |MERGE INTO db1.tbl AS target
          |USING db2.tbl AS source
          |ON target.col1 = source.col1
          |WHEN MATCHED AND (target.col2='delete') THEN DELETE
          |WHEN MATCHED AND (target.col2='update1') THEN UPDATE SET target.col2 = source.col2
          |WHEN MATCHED AND (target.col2='update2') THEN UPDATE SET target.col2 = source.col2
          |WHEN NOT MATCHED AND (target.col2='insert')
          |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        """.stripMargin)
    }

    assert(exc.getMessage.contains("There should be at most 2 'WHEN MATCHED' clauses."))
  }

  test("merge into table: at most one not matched clause") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |MERGE INTO db1.tbl AS target
          |USING db2.tbl AS source
          |ON target.col1 = source.col1
          |WHEN MATCHED AND (target.col2='delete') THEN DELETE
          |WHEN MATCHED AND (target.col2='update1') THEN UPDATE SET target.col2 = source.col2
          |WHEN NOT MATCHED AND (target.col2='insert1')
          |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
          |WHEN NOT MATCHED AND (target.col2='insert2')
          |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        """.stripMargin)
    }

    assert(exc.getMessage.contains("There should be at most 1 'WHEN NOT MATCHED' clause."))
  }

  test("merge into table: the first matched clause must have a condition if there's a second") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |MERGE INTO db1.tbl AS target
          |USING db2.tbl AS source
          |ON target.col1 = source.col1
          |WHEN MATCHED THEN DELETE
          |WHEN MATCHED THEN UPDATE SET target.col2 = source.col2
          |WHEN NOT MATCHED AND (target.col2='insert')
          |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        """.stripMargin)
    }

    assert(exc.getMessage.contains("the first MATCHED clause must have a condition"))
  }

  test("merge into table: there must be a when (not) matched condition") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |MERGE INTO db1.tbl AS target
          |USING db2.tbl AS source
          |ON target.col1 = source.col1
        """.stripMargin)
    }

    assert(exc.getMessage.contains("There must be at least one WHEN clause in a MERGE statement"))
  }

  test("merge into table: there can be only a single use DELETE or UPDATE") {
    Seq("UPDATE SET *", "DELETE").foreach { op =>
      val exc = intercept[ParseException] {
        parsePlan(
          s"""
             |MERGE INTO db1.tbl AS target
             |USING db2.tbl AS source
             |ON target.col1 = source.col1
             |WHEN MATCHED AND (target.col2='delete') THEN $op
             |WHEN MATCHED THEN $op
             |WHEN NOT MATCHED AND (target.col2='insert')
             |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
           """.stripMargin)
      }

      assert(exc.getMessage.contains(
        "UPDATE and DELETE can appear at most once in MATCHED clauses"))
    }
  }

  private def parseCompare(sql: String, expected: LogicalPlan): Unit = {
    comparePlans(parsePlan(sql), expected, checkAnalysis = false)
  }
}
