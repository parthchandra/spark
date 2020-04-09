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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.sql.{sources, Strategy}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, Expression, IsNotNull, IsNull, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, Repartition}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, ProjectionOverSchema, SelectedField, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousCoalesceExec, WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

object DataSourceV2Strategy extends Strategy {

  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  private def pushFilters(
      reader: DataSourceReader,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    reader match {
      case r: SupportsPushDownFilters =>
        // A map from translated data source filters to original catalyst filter expressions.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated = DataSourceStrategy.translateFilter(filterExpr)
          if (translated.isDefined) {
            translatedFilterToExpr(translated.get) = filterExpr
          } else {
            untranslatableExprs += filterExpr
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilterToExpr.keys.toArray)
          .map(translatedFilterToExpr)
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map(translatedFilterToExpr)
        (pushedFilters, untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return new output attributes after column pruning.
   */
  private def pruneColumns(
      reader: DataSourceReader,
      relation: DataSourceV2Relation,
      exprs: Seq[Expression]): Seq[AttributeReference] = {
    reader match {
      case r: SupportsPushDownRequiredColumns if SQLConf.get.nestedSchemaPruningEnabled =>
        val requiredRootFields = identifyRootFields(exprs)
        if (requiredRootFields.exists { root: RootField => !root.derivedFromAtt }) {
          val relationSchema = relation.output.toStructType
          val prunedRelationSchema = pruneRelationSchema(relationSchema, requiredRootFields)
          // If the data schema is different from the pruned data schema, continue. Otherwise,
          // return the full relation output. We effect this comparison by counting the number
          // of "leaf" fields in each schemata, assuming the fields in prunedDataSchema are
          // a subset of the fields in relationSchema.
          if (countLeaves(relationSchema) > countLeaves(prunedRelationSchema)) {
            r.pruneColumns(prunedRelationSchema)
            val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
            r.readSchema().toAttributes.map {
              // We have to keep the attribute id during transformation.
              a => a.withExprId(nameToAttr(a.name).exprId)
            }
          } else {
            relation.output
          }
        } else {
          // try non-nested schema pruning
          val requiredColumns = AttributeSet(exprs.flatMap(_.references))
          val neededOutput = relation.output.filter(requiredColumns.contains)
          if (neededOutput != relation.output) {
            r.pruneColumns(neededOutput.toStructType)
            val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
            r.readSchema().toAttributes.map {
              // We have to keep the attribute id during transformation.
              a => a.withExprId(nameToAttr(a.name).exprId)
            }
          } else {
            relation.output
          }
        }
      case r: SupportsPushDownRequiredColumns =>
        val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val neededOutput = relation.output.filter(requiredColumns.contains)
        if (neededOutput != relation.output) {
          r.pruneColumns(neededOutput.toStructType)
          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          r.readSchema().toAttributes.map {
            // We have to keep the attribute id during transformation.
            a => a.withExprId(nameToAttr(a.name).exprId)
          }
        } else {
          relation.output
        }

      case _ => relation.output
    }
  }


  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>
      val (normalizedProjects, normalizedFilters) = normalizeAttributeRefNames(
        relation, project, filters)
      val reader = relation.newReader()
      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFilters) = pushFilters(reader, normalizedFilters)
      val output = pruneColumns(reader, relation, normalizedProjects ++ postScanFilters)
      logInfo(
        s"""
           |Pushing operators to ${relation.source.getClass}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
           |Output type: ${output.toStructType.catalogString}
         """.stripMargin)

      val scan = DataSourceV2ScanExec(
        output, relation.source, relation.options, pushedFilters, reader)

      val projectionOverSchema = ProjectionOverSchema(output.toStructType)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      val withFilter = filterCondition
        .map(_.transformDown {
          case projectionOverSchema(expr) => expr
        })
        .map(FilterExec(_, scan))
        .getOrElse(scan)

      val newProjects = normalizedProjects
        .map(_.transformDown {
          case projectionOverSchema(expr) => expr
        })
        .map { case expr: NamedExpression => expr }

      // always add the projection, which will produce unsafe rows required by some operators
      ProjectExec(newProjects, withFilter) :: Nil

    case r: StreamingDataSourceV2Relation =>
      // ensure there is a projection, which will produce unsafe rows required by some operators
      ProjectExec(r.output,
        DataSourceV2ScanExec(r.output, r.source, r.options, r.pushedFilters, r.reader)) :: Nil

    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case AppendData(r: DataSourceV2Relation, query, _) =>
      WriteToDataSourceV2Exec(r.newWriter(), planLater(query)) :: Nil

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case Repartition(1, false, child) =>
      val isContinuous = child.collectFirst {
        case StreamingDataSourceV2Relation(_, _, _, r: ContinuousReader) => r
      }.isDefined

      if (isContinuous) {
        ContinuousCoalesceExec(1, planLater(child)) :: Nil
      } else {
        Nil
      }

    case _ => Nil
  }

  /**
   * Returns the set of fields that the query plan needs.
   */
  private def identifyRootFields(exprs: Seq[Expression]): Seq[RootField] = {
    val rootFields = exprs.flatMap(getRootFields)

    // Kind of expressions don't need to access any fields of a root fields, e.g., `IsNotNull`.
    // For them, if there are any nested fields accessed in the query, we don't need to add root
    // field access of above expressions.
    // For example, for a query `SELECT name.first FROM contacts WHERE name IS NOT NULL`,
    // we don't need to read nested fields of `name` struct other than `first` field.
    val (requiredRootFields, optRootFields) = rootFields
      .distinct
      .partition(!_.prunedIfAnyChildAccessed)

    optRootFields.filter { opt =>
      !requiredRootFields.exists { root =>
        root.field.name == opt.field.name && {
          // Checking if current optional root field can be pruned.
          // For each required root field, we merge it with the optional root field:
          // 1. If this optional root field has nested fields and any nested field of it is used
          //    in the query, the merged field type must equal to the optional root field type.
          //    We can prune this optional root field. For example, for optional root field
          //    `struct<name:struct<middle:string,last:string>>`, if its field
          //    `struct<name:struct<last:string>>` is used, we don't need to add this optional
          //    root field.
          // 2. If this optional root field has no nested fields, the merged field type equals
          //    to the optional root field only if they are the same. If they are, we can prune
          //    this optional root field too.
          val rootFieldType = StructType(Array(root.field))
          val optFieldType = StructType(Array(opt.field))
          val merged = optFieldType.merge(rootFieldType)
          merged.sameType(optFieldType)
        }
      }
    } ++ requiredRootFields
  }

  /**
   * Gets the root (aka top-level, no-parent) [[StructField]]s for the given [[Expression]].
   * When expr is an [[Attribute]], construct a field around it and indicate that that
   * field was derived from an attribute.
   */
  private def getRootFields(expr: Expression): Seq[RootField] = {
    expr match {
      case att: Attribute =>
        RootField(StructField(att.name, att.dataType, att.nullable), derivedFromAtt = true) :: Nil
      case SelectedField(field) => RootField(field, derivedFromAtt = false) :: Nil
      // Root field accesses by `IsNotNull` and `IsNull` are special cases as the expressions
      // don't actually use any nested fields. These root field accesses might be excluded later
      // if there are any nested fields accesses in the query plan.
      case IsNotNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, prunedIfAnyChildAccessed = true) :: Nil
      case IsNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, prunedIfAnyChildAccessed = true) :: Nil
      case IsNotNull(_: Attribute) | IsNull(_: Attribute) =>
        expr.children.flatMap(getRootFields).map(_.copy(prunedIfAnyChildAccessed = true))
      case _ =>
        expr.children.flatMap(getRootFields)
    }
  }

  /**
   * This represents a "root" schema field (aka top-level, no-parent). `field` is the
   * `StructField` for field name and datatype. `derivedFromAtt` indicates whether it
   * was derived from an attribute or had a proper child. `prunedIfAnyChildAccessed` means
   * whether this root field can be pruned if any of child field is used in the query.
   */
  private case class RootField(
      field: StructField,
      derivedFromAtt: Boolean,
      prunedIfAnyChildAccessed: Boolean = false)

  /**
   * Counts the "leaf" fields of the given dataType. Informally, this is the
   * number of fields of non-complex data type in the tree representation of
   * [[DataType]].
   */
  private def countLeaves(dataType: DataType): Int = dataType match {
    case array: ArrayType => countLeaves(array.elementType)
    case map: MapType => countLeaves(map.keyType) + countLeaves(map.valueType)
    case struct: StructType => struct.map(field => countLeaves(field.dataType)).sum
    case _ => 1
  }

  /**
   * Filters the schema from the given file by the requested fields.
   * Schema field ordering from the file is preserved.
   */
  private def pruneRelationSchema(
      relationSchema: StructType,
      requestedRootFields: Seq[RootField]): StructType = {
    // Merge the requested root fields into a single schema. Note the ordering of the fields
    // in the resulting schema may differ from their ordering in the logical relation's
    // original schema
    val mergedSchema = requestedRootFields
      .map { case root: RootField => StructType(Array(root.field)) }
      .reduceLeft(_ merge _)
    val relationFieldNames = relationSchema.fieldNames.toSet
    val mergedDataSchema =
      StructType(mergedSchema.filter(f => relationFieldNames.contains(f.name)))
    // Sort the fields of mergedDataSchema according to their order in dataSchema,
    // recursively. This makes mergedDataSchema a pruned schema of dataSchema
    sortLeftFieldsByRight(mergedDataSchema, relationSchema).asInstanceOf[StructType]
  }

  /**
   * Sorts the fields and descendant fields of structs in left according to their order in
   * right. This function assumes that the fields of left are a subset of the fields of
   * right, recursively. That is, left is a "subschema" of right, ignoring order of
   * fields.
   */
  private def sortLeftFieldsByRight(left: DataType, right: DataType): DataType =
    (left, right) match {
      case (ArrayType(leftElementType, containsNull), ArrayType(rightElementType, _)) =>
        ArrayType(
          sortLeftFieldsByRight(leftElementType, rightElementType),
          containsNull)
      case (MapType(leftKeyType, leftValueType, containsNull),
      MapType(rightKeyType, rightValueType, _)) =>
        MapType(
          sortLeftFieldsByRight(leftKeyType, rightKeyType),
          sortLeftFieldsByRight(leftValueType, rightValueType),
          containsNull)
      case (leftStruct: StructType, rightStruct: StructType) =>
        val filteredRightFieldNames = rightStruct.fieldNames.filter(leftStruct.fieldNames.contains)
        val sortedLeftFields = filteredRightFieldNames.map { fieldName =>
          val leftFieldType = leftStruct(fieldName).dataType
          val rightFieldType = rightStruct(fieldName).dataType
          val sortedLeftFieldType = sortLeftFieldsByRight(leftFieldType, rightFieldType)
          StructField(fieldName, sortedLeftFieldType)
        }
        StructType(sortedLeftFields)
      case _ => left
    }

  /**
   * Normalizes the names of the attribute references in the given projects and filters to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(
      relation: DataSourceV2Relation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): (Seq[NamedExpression], Seq[Expression]) = {
    val normalizedAttNameMap = relation.output.map(att => (att.exprId, att.name)).toMap
    val normalizedProjects = projects.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    }).map { case expr: NamedExpression => expr }
    val normalizedFilters = filters.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    })
    (normalizedProjects, normalizedFilters)
  }
}
