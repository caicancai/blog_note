# 背景

Spark Sql支持多种数据源，如何将UDF下推对应的数据源进行计算。从Spark源码来看Spark是留给了用户了自定义拓展UDF下推的空间，下面就让我们来看一下

# 详细方案

## 注册UDF

### registerFuntion

在注册UDF之前你需要在对应JDBCDialect下，提供一个registerFuntion函数，将对应的UDF塞入对应的functionMap之中

```scala
def registerFunction(name: String, fn: UnboundFunction): UnboundFunction = {
    functionMap.put(name, fn)
  }
```

### 声明UDF的基本信息

需要把UDF的UDF的入参，结果，名称等基本信息声明清楚，以便后面解析校验等，e.g

```scala
case object CharLength extends ScalarFunction[Int] {
    override def inputTypes(): Array[DataType] = Array(StringType)

    override def resultType(): DataType = IntegerType

    override def name(): String = "CHAR_LENGTH"

    override def canonicalName(): String = "mysql.char_length"

    override def produceResult(input: InternalRow): Int = {
      val s = input.getString(0)
      s.length
    }
  }
```

### UDF参数校验

需要对UDF的参数进行校验以及对参数进行描述，在Spark中可以继承UnboundFunction实现对UDF参数的校验，e.g

```scala
case class StrLen(impl: BoundFunction) extends UnboundFunction {
  override def name(): String = "strlen"

  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.fields.length != 1) {
      throw new UnsupportedOperationException("Expect exactly one argument");
    }
    inputType.fields(0).dataType match {
      case StringType => impl
      case _ =>
        throw new UnsupportedOperationException("Expect StringType")
    }
  }

  override def description(): String =
    "strlen: returns the length of the input string  strlen(string) -> int"
}
```

# 解析

那如何保证Spark能正确解析这个UDF呢，在这里你需要重写JDBCSQLBuilder的一些基本方法去保证注册的UDF能用过校验，详细代码如下

```scala
val testMysqlDialect = new JdbcDialect {
    override def canHandle(url: String): Boolean = MySQLDialect.canHandle(url)

    class MysqlBuilder extends JDBCSQLBuilder {
      override def visitUserDefinedScalarFunction(funcName: String，canonicalName: String, inputs: Array[String]): String = {
        canonicalName match {
          case "mysql.char_length" =>
            s"$funcName(${inputs.mkString(", ")})"
          case _ => super.visitUserDefinedScalarFunction(funcName, canonicalName, inputs)
        }
      }
    }

    override def compileExpression(expr: Expression): Option[String] = {
      val MySQLBuilder = new MysqlBuilder()
      try {
        Some(MySQLBuilder.build(expr))
      } catch {
        case NonFatal(e) =>
          logWarning("Error occurs while compiling V2 expression", e)
          None
      }
    }

    override def functions: Seq[(String, UnboundFunction)] = MySQLDialect.functions
  }
```

## Catalog

parser过后就会生成Unsloved Logical Plan，我们需要将Unsloved Locial Plan转换成Logical Plan，在这里由于Spark默认走的是H2的语法，所以在这里需要适配一下对应数据源的语法，比如h2是可以“test”.”people”这种骚操作的，我们需要对此进行修改

```scala
override def quoteIdentifier(colName: String): String = {
      s"`$colName`"
    }
```

## PushDown

当生成的Logical Plan没有问题后，我们就可以走Spark原先的下推逻辑了，在Spark原先的下推逻辑中，就有一系列的下推规则，这里列一下V2的

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

那如何走呢，举个例子，有一条这样的SQL，我注册的函数是my_strlen(CharLength)

```sql
SELECT * FROM h2.test.people where mysql.my_strlen(name) > 2
```

在走下推过程中，由于改UDF是在where条件中，改udf会走pushdownfilters的下推规则，将该函数成功下推，这里的源码几乎不用改。

至此，我们就将我们的UDF成功下推到MySQL数据源了

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/492206df-c13c-4c16-a762-e7c31afd8176/5609b247-3fc3-4302-b40f-37fa22c68847/Untitled.png)

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/492206df-c13c-4c16-a762-e7c31afd8176/db3b8e11-4646-4b56-a7bf-ab65a0a7c85d/Untitled.png)

测试代码

```scala
package org.apache.spark.sql.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.StrLen
import org.apache.spark.sql.connector.catalog.functions.{ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql._
import org.apache.spark.util.Utils

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.util.control.NonFatal

class JDBCV2MysqlSuite extends QueryTest with SharedSparkSession with ExplainSuiteHelper {

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:mysql://192.168.83.177:9030/test?user=root&password=123456"
  val testBytes = Array[Byte](99.toByte, 134.toByte, 135.toByte, 200.toByte, 205.toByte) ++
    Array.fill(15)(0.toByte)

  val testMysqlDialect = new JdbcDialect {
    override def canHandle(url: String): Boolean = MySQLDialect.canHandle(url)

    override def quoteIdentifier(colName: String): String = {
      s"`$colName`"
    }

    class MysqlBuilder extends JDBCSQLBuilder {
      override def visitUserDefinedScalarFunction(
                                                   funcName: String,
                                                   canonicalName: String,
                                                   inputs: Array[String]): String = {
        canonicalName match {
          case "mysql.char_length" =>
            s"$funcName(${inputs.mkString(", ")})"
          case _ => super.visitUserDefinedScalarFunction(funcName, canonicalName, inputs)
        }
      }
    }

    override def compileExpression(expr: Expression): Option[String] = {
      val MySQLBuilder = new MysqlBuilder()
      try {
        Some(MySQLBuilder.build(expr))
      } catch {
        case NonFatal(e) =>
          logWarning("Error occurs while compiling V2 expression", e)
          None
      }
    }

    override def functions: Seq[(String, UnboundFunction)] = MySQLDialect.functions
  }
  case object CharLength1 extends ScalarFunction[Int] {
    override def inputTypes(): Array[DataType] = Array(StringType)

    override def resultType(): DataType = IntegerType

    override def name(): String = "CHAR_LENGTH"

    override def canonicalName(): String = "mysql.char_length"

    override def produceResult(input: InternalRow): Int = {
      val s = input.getString(0)
      s.length
    }
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.mysql.url", url)
    .set("spark.sql.catalog.mysql.driver", "com.mysql.cj.jdbc.Driver")
    .set("spark.sql.catalog.mysql.pushDownAggregate", "true")
    .set("spark.sql.catalog.mysql.pushDownLimit", "true")
    .set("spark.sql.catalog.mysql.pushDownOffset", "true")

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("com.mysql.cj.jdbc.Driver")
    withConnection { conn =>
    }
    MySQLDialect.registerFunction("my_strlen", StrLen(CharLength1))
  }

  override def afterAll(): Unit = {
    H2Dialect.clearFunctions()
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  private def checkPushedInfo(df: DataFrame, expectedPlanFragment: String*): Unit = {
    withSQLConf(SQLConf.MAX_METADATA_STRING_LENGTH.key -> "1000") {
      df.queryExecution.optimizedPlan.collect {
        case _: DataSourceV2ScanRelation =>
          checkKeywordsExistsInExplain(df, expectedPlanFragment: _*)
      }
    }
  }

  test("scan with filter push-down with UDF mysql") {
    JdbcDialects.unregisterDialect(MySQLDialect)
    try {
      JdbcDialects.registerDialect(testMysqlDialect)
      val df1 = sql("SELECT * FROM mysql.test.people where mysql.my_strlen(name) > 2")
      checkPushedInfo(df1, "PushedFilters: [CHAR_LENGTH(name) > 2],")
      checkAnswer(df1, Seq(Row("fred", 1), Row("mary", 2)))
    } finally {
      JdbcDialects.unregisterDialect(testMysqlDialect)
      JdbcDialects.registerDialect(MySQLDialect)
    }
  }

}
```
