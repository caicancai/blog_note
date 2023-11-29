# Spark Sql下推预览

## 架构预览

![1.2.jpg](https://github.com/caicancai/spark-sql-2.3-source-code-interpretation/blob/master/assets/1.2.jpg?raw=true)

大致分为六步：

1. sql 语句经过 SqlParser 解析成 Unresolved Logical Plan;
2. analyzer 结合 catalog 进行绑定,生成 Logical Plan;
3. optimizer 对 Logical Plan 优化,生成 Optimized LogicalPlan;
4. SparkPlan 将 Optimized LogicalPlan 转换成 Physical Plan;
5. prepareForExecution()将 Physical Plan 转换成 executed Physical Plan;
6. execute()执行可执行物理计划，得到RDD;



## Unresolved Logical Plan -> Logical Plan

### 什么是unresolved logical plan？

unresolved logical plan就是没有和analyzer结合catalog绑定信息生成的假logical plan，例如：

```shell
'Project [unresolvedalias('MAX('ID), None), unresolvedalias('AVG('ID), None)]
+- 'Filter ('id > 0)
   +- 'UnresolvedRelation [h2, test, people], [], false

```

在这个logical plan可以很明显看到unresolved等标签

### 那unresolved logical plan如何和analyzer结合catalog去生成logicalplan呢？

上面的unresolved logical plan将会执行如下代码进去analyze的过程：

```scala
lazy val analyzed: LogicalPlan = {
  SparkSession.setActiveSession(sparkSession)
  sparkSession.sessionState.analyzer.executeAndCheck(logical)
}
```

执行analyze的过程中会执行一系列的rule规则，简单来说就是一个rule规则是输入为旧的logical plan，输出为新的logical plan，仅此而已，大致代码如下：

```scala
def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    val queryExecutionMetrics = RuleExecutor.queryExecutionMeter
    val planChangeLogger = new PlanChangeLogger[TreeType]()
    val tracker: Option[QueryPlanningTracker] = QueryPlanningTracker.get
    val beforeMetrics = RuleExecutor.getCurrentMetrics()

    val enableValidation = SQLConf.get.getConf(SQLConf.PLAN_CHANGE_VALIDATION)
    // Validate the initial input.
    if (Utils.isTesting || enableValidation) {
      validatePlanChanges(plan, plan) match {
        case Some(msg) =>
          val ruleExecutorName = this.getClass.getName.stripSuffix("$")
          throw new SparkException(
            errorClass = "PLAN_VALIDATION_FAILED_RULE_EXECUTOR",
            messageParameters = Map("ruleExecutor" -> ruleExecutorName, "reason" -> msg),
            cause = null)
        case _ =>
      }
    }

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            val effective = !result.fastEquals(plan)

            if (effective) {
              queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName)
              queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime)
              planChangeLogger.logRule(rule.ruleName, plan, result)
              // Run the plan changes validation after each rule.
              if (Utils.isTesting || enableValidation) {
                validatePlanChanges(plan, result) match {
                  case Some(msg) =>
                    throw new SparkException(
                      errorClass = "PLAN_VALIDATION_FAILED_RULE_IN_BATCH",
                      messageParameters = Map(
                        "rule" -> rule.ruleName,
                        "batch" -> batch.name,
                        "reason" -> msg),
                      cause = null)
                  case _ =>
                }
              }
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)

            // Record timing information using QueryPlanningTracker
            tracker.foreach(_.recordRuleInvocation(rule.ruleName, runTime, effective))

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val endingMsg = if (batch.strategy.maxIterationsSetting == null) {
              "."
            } else {
              s", please set '${batch.strategy.maxIterationsSetting}' to a larger value."
            }
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}" +
              s"$endingMsg"
            if (Utils.isTesting || batch.strategy.errorOnExceed) {
              throw new RuntimeException(message)
            } else {
              logWarning(message)
            }
          }
          // Check idempotence for Once batches.
          if (batch.strategy == Once &&
            Utils.isTesting && !excludedOnceBatches.contains(batch.name)) {
            checkBatchIdempotence(batch, curPlan)
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      planChangeLogger.logBatch(batch.name, batchStartPlan, curPlan)
    }
    planChangeLogger.logMetrics(RuleExecutor.getCurrentMetrics() - beforeMetrics)

    curPlan
  }
}
```

### 那如何判断unresolved logical plan进入哪个规则后出来的是新的logical plan呢？

其实可以观察一开始的unresolved logical plan的数据结构，以上面的unresolved logical plan为例：

```shell
'Project [unresolvedalias('MAX('ID), None), unresolvedalias('AVG('ID), None)]
+- 'Filter ('id > 0)
   +- 'UnresolvedRelation [h2, test, people], [], false
```

我们可以观察到有两个地方是有unresolve标记的一个是Unresolvedalias和UnresolvedRelation，而rule规则中恰好有两个规则

①进入UnresolvedRelation后

```shell
'Project [unresolvedalias('MAX('ID), None), unresolvedalias('AVG('ID), None)]
+- 'Filter ('id > 0)
   +- SubqueryAlias h2.test.people
      +- RelationV2[NAME#0, ID#1] h2.test.people test.people
```

②进入Unresolvedalias规则后

```shell
'Project [max(ID#1) AS max(ID)#4, avg(ID#1) AS avg(ID)#5]
+- Filter (id#1 > 0)
   +- SubqueryAlias h2.test.people
      +- RelationV2[NAME#0, ID#1] h2.test.people test.people
```

经过一系列规则后生成的logical plan

```shell
Aggregate [max(ID#1) AS max(ID)#4, avg(ID#1) AS avg(ID)#5]
+- Filter (id#1 > 0)
   +- SubqueryAlias h2.test.people
      +- RelationV2[NAME#0, ID#1] h2.test.people test.people
```

可以发现这时候已经生成和catalog绑定后的logical plan了

所以答案很明显了，进入每个rule规则后会有对应判断看执不执行那个rule规则



## Logical Plan ->  Optimized LogicalPlan

从logical plan到optimized LogicalPlan也是会走一系列的rule规则去优化

#### 以agg算子的下推为例

在rule规则中有一个V2ScanRelationPushDown下推规则，会进行各种算子的下推

```scala
  def apply(plan: LogicalPlan): LogicalPlan = {
    val pushdownRules = Seq[LogicalPlan => LogicalPlan] (
      createScanBuilder,
      pushDownSample,
      pushDownFilters,
      pushDownAggregates,
      pushDownLimitAndOffset,
      buildScanWithPushedAggregate,
      pruneColumns)

    pushdownRules.foldLeft(plan) { (newPlan, pushDownRule) =>
      pushDownRule(newPlan)
    }
  }
```

在到达V2ScanRelationPushDown已经经过一些优化规则，变成新的Logical Plan如下：

```shell
Aggregate [max(ID#1) AS max(ID)#4, avg(ID#1) AS avg(ID)#5]
+- Project [ID#1]
   +- Filter (isnotnull(id#1) AND (id#1 > 0))
      +- RelationV2[NAME#0, ID#1] h2.test.people test.people
```

其中有一个pushDownAggregates就是把agg算子下推，具体步骤如下

1. 解析logical plan为对应的数据结构Aggregate并判断，如果是则重写Aggregate
2. 将Aggregate数据结构进行拆分成对应的数据结构
3. 根据对应的数据结构进行判断是否能进行下推
4. 正式走下推优化的逻辑并返回对应的数据结构

```scala
private def rewriteAggregate(agg: Aggregate): LogicalPlan = agg.child match {
    case PhysicalOperation(project, Nil, holder @ ScanBuilderHolder(_, _,
        r: SupportsPushDownAggregates)) if CollapseProject.canCollapseExpressions(
        agg.aggregateExpressions, project, alwaysInline = true) =>
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
      if (translatedAggOpt.isEmpty) {
        // Cannot translate the catalyst aggregate, return the query plan unchanged.
        return agg
      }

      val (finalResultExprs, finalAggExprs, translatedAgg, canCompletePushDown) = {
        if (r.supportCompletePushDown(translatedAggOpt.get)) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, true)
        } else if (!translatedAggOpt.get.aggregateExpressions().exists(_.isInstanceOf[Avg])) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
        } else {
          // scalastyle:off
          // The data source doesn't support the complete push-down of this aggregation.
          // Here we translate `AVG` to `SUM / COUNT`, so that it's more likely to be
          // pushed, completely or partially.
          // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
          // SELECT avg(c1) FROM t GROUP BY c2;
          // The original logical plan is
          // Aggregate [c2#10],[avg(c1#9) AS avg(c1)#19]
          // +- ScanOperation[...]
          //
          // After convert avg(c1#9) to sum(c1#9)/count(c1#9)
          // we have the following
          // Aggregate [c2#10],[sum(c1#9)/count(c1#9) AS avg(c1)#19]
          // +- ScanOperation[...]
          // scalastyle:on
          val newResultExpressions = actualResultExprs.map { expr =>
            expr.transform {
              case AggregateExpression(avg: aggregate.Average, _, isDistinct, _, _) =>
                val sum = aggregate.Sum(avg.child).toAggregateExpression(isDistinct)
                val count = aggregate.Count(avg.child).toAggregateExpression(isDistinct)
                avg.evaluateExpression transform {
                  case a: Attribute if a.semanticEquals(avg.sum) =>
                    addCastIfNeeded(sum, avg.sum.dataType)
                  case a: Attribute if a.semanticEquals(avg.count) =>
                    addCastIfNeeded(count, avg.count.dataType)
                }
            }
          }.asInstanceOf[Seq[NamedExpression]]
          // Because aggregate expressions changed, translate them again.
          aggExprToOutputOrdinal.clear()
          val newAggregates =
            collectAggregates(newResultExpressions, aggExprToOutputOrdinal)
          val newNormalizedAggExprs = DataSourceStrategy.normalizeExprs(
            newAggregates, holder.relation.output).asInstanceOf[Seq[AggregateExpression]]
          val newTranslatedAggOpt = DataSourceStrategy.translateAggregation(
            newNormalizedAggExprs, normalizedGroupingExpr)
          if (newTranslatedAggOpt.isEmpty) {
            // Ideally we should never reach here. But if we end up with not able to translate
            // new aggregate with AVG replaced by SUM/COUNT, revert to the original one.
            (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
          } else {
            (newResultExpressions, newNormalizedAggExprs, newTranslatedAggOpt.get,
              r.supportCompletePushDown(newTranslatedAggOpt.get))
          }
        }
      }

      if (!canCompletePushDown && !supportPartialAggPushDown(translatedAgg)) {
        return agg
      }
      if (!r.pushAggregation(translatedAgg)) {
        return agg
      }

      // scalastyle:off
      // We name the output columns of group expressions and aggregate functions by
      // ordinal: `group_col_0`, `group_col_1`, ..., `agg_func_0`, `agg_func_1`, ...
      // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
      // SELECT min(c1), max(c1) FROM t GROUP BY c2;
      // Use group_col_0, agg_func_0, agg_func_1 as output for ScanBuilderHolder.
      // We want to have the following logical plan:
      // == Optimized Logical Plan ==
      // Aggregate [group_col_0#10], [min(agg_func_0#21) AS min(c1)#17, max(agg_func_1#22) AS max(c1)#18]
      // +- ScanBuilderHolder[group_col_0#10, agg_func_0#21, agg_func_1#22]
      // Later, we build the `Scan` instance and convert ScanBuilderHolder to DataSourceV2ScanRelation.
      // scalastyle:on
      val groupOutputMap = normalizedGroupingExpr.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"group_col_$i", e.dataType)() -> e
      }
      val groupOutput = groupOutputMap.unzip._1
      val aggOutputMap = finalAggExprs.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"agg_func_$i", e.dataType)() -> e
      }
      val aggOutput = aggOutputMap.unzip._1
      val newOutput = groupOutput ++ aggOutput
      val groupByExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      normalizedGroupingExpr.zipWithIndex.foreach { case (expr, ordinal) =>
        if (!groupByExprToOutputOrdinal.contains(expr.canonicalized)) {
          groupByExprToOutputOrdinal(expr.canonicalized) = ordinal
        }
      }

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
  }
```

经过重写后的Aggregate数据结构如下：

```shell
Project [agg_func_0#12 AS max(ID#1)#2 AS max(ID)#4, agg_func_1#13 AS avg(ID#1)#3 AS avg(ID)#5]
+- ScanBuilderHolder [agg_func_0#12, agg_func_1#13], RelationV2[NAME#0, ID#1] h2.test.people test.people, JDBCScanBuilder(org.apache.spark.sql.test.TestSparkSession@5edaa572,StructType(StructField(NAME,StringType,true),StructField(ID,IntegerType,true)),org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions@460df441)
```

最后生成Optimized LogicalPlan如下：

```shell
Project [MAX(ID)#14 AS max(ID#1)#2 AS max(ID)#4, AVG(ID)#15 AS avg(ID#1)#3 AS avg(ID)#5]
+- RelationV2[MAX(ID)#14, AVG(ID)#15] test.people
```



**后续：详细剖析Spark Sql的V2ScanRelationPushDown下推优化逻辑**