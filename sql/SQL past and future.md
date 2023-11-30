# SQL past and future

Ken North 在 Dr. Dobb’s Journal 中撰文，对 SQL 悠久而传奇的历史进行了很好的概述。 这篇文章有助于人们了解大型数据库供应商之间的合并浪潮，并了解数据库和类似数据库的软件的当前趋势。 我想就 SQL 和数据库管理系统的发展方向提出我的看法。

诺斯调查了“数据库已死”的说法，并发现有关其已死的报道再次被大大夸大了：

> Forrester Research 最近估计整个数据库市场（许可、支持、咨询）将从 2009 年的 270 亿美元增长到 2012 年的 320 亿美元。SQL 技术在许多组织和数百万个网站中得到了根深蒂固的支持。 也许这可以解释为什么在过去十年中，IBM、甲骨文、Sun 和 SAP 在“死”技术上投入了数十亿美元。

然而，我确实相信关系数据库目前正处于危机之中。 二十多年来，关系数据库一直是数据管理的支柱，但 Oracle 及其同伙对“大数据”（来自网络和传感器的海量信息）没有答案。

NoSQL 运动正在通过挑战 RDBMS 供应商的一些假设来解决这些问题。 在 SQLstream，我们将自己视为 NoSQL 运动的一部分，尽管我们是 SQL 的忠实粉丝，因为我们正在挑战其中最大的假设：您必须将数据放在磁盘上才能进行分析。

遗憾的是，North 没有提到流式 SQL，因为它完全符合 SQL 语言的宏伟弧线：采用新问题，以声明方式表达它们，首先使用专用数据库引擎解决它们，最后通过适应 SQL 的架构 大型通用数据库引擎。 最后一步有时需要很多年才能完成，但它发生在事务处理、对象数据库和数据仓库中，而且我毫不怀疑它也会发生在流关系数据中。

SQL 保持相关性的原因之一是 SQL 标准流程； 基于一个数据库构建的产品可以在另一个数据库上运行，也许更重要的是，在一个引擎上获得的技能集可以应用于另一个引擎。 当尘埃落定，大型数据库已经吸取了艰难的架构教训时，我认为许多新问题将在 SQL 中得到解决。

与 Mike Stonebraker 不同，我确实认为组织会希望将所有这些不同形式的数据放入一个数据库管理系统中。 该数据库当然将是分布在许多服务器、磁盘、数据组织和查询处理引擎上的外观，但将提供集中管理并允许组合不同形式的数据。 他们会如愿以偿，因为 SQL 语言在隐藏底层数据组织差异方面非常强大。

当尘埃落定后，SQL 语言将再次发生变化和适应，也许数据库供应商名册上会出现一些新名字，但我们将再次使用声明式解决大部分数据管理问题 以“SELECT”一词开头的查询。 SQL 已死； SQL万岁！