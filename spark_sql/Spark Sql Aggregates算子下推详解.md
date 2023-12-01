# Spark Sql Aggregates算子下推详解

## 1.前置准备

让我们看一下进入V2ScanRelation的Logical Plan

```shell
Aggregate [count(DEPT#0) AS count(DEPT)#6L]
+- Project [DEPT#0]
   +- RelationV2[DEPT#0, NAME#1, SALARY#2, BONUS#3, IS_MANAGER#4] h2.test.employee test.employee
```

首先进入createScanBuilder，在这里会对生成一个ScanBuilderHolder，如下（具体有什么用，我也母鸡）：

```shell
Aggregate [count(DEPT#0) AS count(DEPT)#6L]
+- Project [DEPT#0]
   +- ScanBuilderHolder [DEPT#0, NAME#1, SALARY#2, BONUS#3, IS_MANAGER#4], RelationV2[DEPT#0, NAME#1, SALARY#2, BONUS#3, IS_MANAGER#4] h2.test.employee test.employee, JDBCScanBuilder(org.apache.spark.sql.test.TestSparkSession@3b3a3a92,StructType(StructField(DEPT,IntegerType,true),StructField(NAME,StringType,true),StructField(SALARY,DecimalType(20,2),true),StructField(BONUS,DoubleType,true),StructField(IS_MANAGER,BooleanType,true)),org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions@7a7bd9a8)
```

经过pushdownSample和pushdownfilters后，就会进入pushDownAggregates下推规则

```scala
    val pushdownRules = Seq[LogicalPlan => LogicalPlan] (
      createScanBuilder,
      pushDownSample,
      pushDownFilters,
      pushDownAggregates,
      pushDownLimitAndOffset,
      buildScanWithPushedAggregate,
      pruneColumns)
```



## 2.pushDownAggregates

### **1.**在这里会解析Logical Plan判断有无对应的aggregate数据结构

```scala
  def pushDownAggregates(plan: LogicalPlan): LogicalPlan = plan.transform {
    // update the scan builder with agg pushdown and return a new plan with agg pushed
    case agg: Aggregate => rewriteAggregate(agg)
  }
```

上图明显是有agg算子对应的数据结构的，所以会继续走下面重写aggregate数据结构的具体流程

```shell
Aggregate [count(DEPT#0) AS count(DEPT)#6L]
```

### **2.**将对应的数据结构进行拆分，用于后续下推规则的判断

下面我们详细刨析一下这些数据是什么

① aliasMap ：从project提取别名及其投影存储到map中，例如：

```shell
'SELECT a + b AS c, d ...' produces Map(c -> Alias(a + b, c))
```

② actualResultExprs和actualGroupExprs：分别保存aggregate和group by表达式对应的数据数据结构

③ aggregates：这里主要是对actualResultExprs中重复的表达式去重，防止下推重复的表达式

```shell
`SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
`max(a)` to the data source.
```

⑤ normalizedAggExprs和normalizedGroupingExpr：更改属性名进行匹配

⑥translatedAggOpt：懂都懂，所以上面的这些都是为了生成这个东西

如果最后translatedAggOpt数据为空，则表示没有需要下推的agg算子

**整体代码如下：**

```scala
      val aliasMap = getAliasMap(project)
      val actualResultExprs = agg.aggregateExpressions.map(replaceAliasButKeepName(_, aliasMap))
      val actualGroupExprs = agg.groupingExpressions.map(replaceAlias(_, aliasMap))

      val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      val aggregates = collectAggregates(actualResultExprs, aggExprToOutputOrdinal)
      val normalizedAggExprs = DataSourceStrategy.normalizeExprs(
        aggregates, holder.relation.output).asInstanceOf[Seq[AggregateExpression]]
      val normalizedGroupingExpr = DataSourceStrategy.normalizeExprs(
        actualGroupExprs, holder.relation.output)
      val translatedAggOpt = DataSourceStrategy.translateAggregation(
        normalizedAggExprs, normalizedGroupingExpr)
```

### **3.**下推规则判断

在spark sql下推判断主要有③点：

①是否能完全下推

 ②是否有avg算子 

③是否能下推到对应的数据源（如果是jdbc）

#### 3.1 如何判断是否能完全下推？

这里的判断的依据主要是单个分区，group by的字段是否是1等

```
  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    lazy val fieldNames = aggregation.groupByExpressions()(0) match {
      case field: FieldReference => field.fieldNames
      case _ => Array.empty[String]
    }
    jdbcOptions.numPartitions.map(_ == 1).getOrElse(true) ||
      (aggregation.groupByExpressions().length == 1 && fieldNames.length == 1 &&
        jdbcOptions.partitionColumn.exists(fieldNames(0).equalsIgnoreCase(_)))
  }
```

#### 3.2 avg算子处理措施

如果存在avg算子，则就将该算子数据结构转换sum/count的形式，然后判断能够完全下推

#### 3.3 判断是否能下推数据源

在这个过程中主要是对agg数据结构的进行解析，并将对应的表达式塞到对应的数据结构中，**用于去执行buildScanWithPushedAggregate操作**

```scala
 override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!jdbcOptions.pushDownAggregate) return false

    val dialect = JdbcDialects.get(jdbcOptions.url)
    val compiledAggs = aggregation.aggregateExpressions.flatMap(dialect.compileAggregate)
    if (compiledAggs.length != aggregation.aggregateExpressions.length) return false

    val compiledGroupBys = aggregation.groupByExpressions.flatMap(dialect.compileExpression)
    if (compiledGroupBys.length != aggregation.groupByExpressions.length) return false

    // The column names here are already quoted and can be used to build sql string directly.
    // e.g. "DEPT","NAME",MAX("SALARY"),MIN("BONUS") =>
    // SELECT "DEPT","NAME",MAX("SALARY"),MIN("BONUS") FROM "test"."employee"
    //   GROUP BY "DEPT", "NAME"
    val selectList = compiledGroupBys ++ compiledAggs
    val groupByClause = if (compiledGroupBys.isEmpty) {
      ""
    } else {
      "GROUP BY " + compiledGroupBys.mkString(",")
    }

    val aggQuery = jdbcOptions.prepareQuery +
      s"SELECT ${selectList.mkString(",")} FROM ${jdbcOptions.tableOrQuery} " +
      s"WHERE 1=0 $groupByClause"
    try {
      finalSchema = JDBCRDD.getQueryOutputSchema(aggQuery, jdbcOptions, dialect)
      pushedAggregateList = selectList
      pushedGroupBys = Some(compiledGroupBys)
      true
    } catch {
      case NonFatal(e) =>
        logError("Failed to push down aggregation to JDBC", e)
        false
    }
  }
```



### 4. 下推Agg算子——重写数据结构

到这一步，就会根据对应的下推规则去重写对应的数据结构

注：这里的重写能完整下推和不能下推的规则是不一样的

```scala
holder.pushedAggregate = Some(translatedAgg)
      holder.pushedAggOutputMap = AttributeMap(groupOutputMap ++ aggOutputMap)
      holder.output = newOutput
      logInfo(
        s"""
           |Pushing operators to ${holder.relation.name}
           |Pushed Aggregate Functions:
           | ${translatedAgg.aggregateExpressions().mkString(", ")}
           |Pushed Group by:
           | ${translatedAgg.groupByExpressions.mkString(", ")}
         """.stripMargin)

      if (canCompletePushDown) {
        val projectExpressions = finalResultExprs.map { expr =>
          expr.transformDown {
            case agg: AggregateExpression =>
              val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
              Alias(aggOutput(ordinal), agg.resultAttribute.name)(agg.resultAttribute.exprId)
            case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
              val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
              expr match {
                case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
                case _ => groupOutput(ordinal)
              }
          }
        }.asInstanceOf[Seq[NamedExpression]]
        Project(projectExpressions, holder)
      } else {
        // scalastyle:off
        // Change the optimized logical plan to reflect the pushed down aggregate
        // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
        // SELECT min(c1), max(c1) FROM t GROUP BY c2;
        // The original logical plan is
        // Aggregate [c2#10],[min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
        // +- RelationV2[c1#9, c2#10] ...
        //
        // After change the V2ScanRelation output to [c2#10, min(c1)#21, max(c1)#22]
        // we have the following
        // !Aggregate [c2#10], [min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
        // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
        //
        // We want to change it to
        // == Optimized Logical Plan ==
        // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
        // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
        // scalastyle:on
        val aggExprs = finalResultExprs.map(_.transform {
          case agg: AggregateExpression =>
            val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
            val aggAttribute = aggOutput(ordinal)
            val aggFunction: aggregate.AggregateFunction =
              agg.aggregateFunction match {
                case max: aggregate.Max =>
                  max.copy(child = aggAttribute)
                case min: aggregate.Min =>
                  min.copy(child = aggAttribute)
                case sum: aggregate.Sum =>
                  // To keep the dataType of `Sum` unchanged, we need to cast the
                  // data-source-aggregated result to `Sum.child.dataType` if it's decimal.
                  // See `SumBase.resultType`
                  val newChild = if (sum.dataType.isInstanceOf[DecimalType]) {
                    addCastIfNeeded(aggAttribute, sum.child.dataType)
                  } else {
                    aggAttribute
                  }
                  sum.copy(child = newChild)
                case _: aggregate.Count =>
                  aggregate.Sum(aggAttribute)
                case other => other
              }
            agg.copy(aggregateFunction = aggFunction)
          case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
            val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
            expr match {
              case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
              case _ => groupOutput(ordinal)
            }
        }).asInstanceOf[Seq[NamedExpression]]
        Aggregate(groupOutput, aggExprs, holder)
      }

    case _ => agg
```

执行完所有下推成功后的数据结构如下

```shell
Project [COUNT(DEPT)#11L AS count(DEPT#0)#5L AS count(DEPT)#6L]
+- RelationV2[COUNT(DEPT)#11L] test.employee
```

### 总结一下

其实logical plan其实相当于把logical plan这个数据结构进行按规则重写了
