10月20日 韩天昊
# 工单04建库建表  完成

一、 数据分层架构与实现
本项目严格遵循了离线数仓的五层架构（ODS -> DIM -> DWD -> DWS -> ADS）思想，实现了数据从原始到应用的逐层加工、清洗、聚合和应用。这种分层设计是现代数据仓库的核心，它带来了诸多优势。

为什么需要分层？
降低复杂度：将复杂的数据处理流程分解为多个简单的步骤。
模块化管理：各层职责分明，便于团队协作和维护。
提高复用性：上层可以复用下层的成果，避免重复开发。
隔离数据问题：某一层的数据或逻辑问题不会直接影响到上层应用。
提升数据质量：通过层层清洗和校验，保证最终数据的准确性。
优化性能：下层进行预聚合，减少上层查询的计算量，提高查询性能。

ODS层
作用：作为数仓的最底层，它是业务系统数据的镜像。主要进行全量或增量抽取，保持数据的原始状态，仅做最基础的存储和分区，防止全表扫描。
技术实现：
存储格式：TEXTFILE，便于接收各种来源的原始数据。
压缩格式：LZO，通过INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'指定，兼顾了压缩比和分片能力，减少数据占用的存储空间，提升查询效率。
分区策略：按天(dt)进行分区，是大数据处理的标准实践。
关键表 :
ods_sku_info：SKU商品表。
ods_order_info：订单表。
ods_payment_info：支付流水表。
ods_comment_info：商品评论表。
ods_base_category3：商品三级分类表。
ods_inventory：商品库存表。
ods_user_info：用户表。
ods_store_info：店铺信息表。
2. DIM层
   作用：存储稳定、变化频率低的维度信息。维度表是用来解释和说明业务事实的“字典表”，它通常与事实表一起构成星型模型。在HQL中，维度就是“用来分组、过滤、看汇总”的那些描述性字段。
   技术实现：
   存储格式：PARQUET，列式存储，非常适合分析型查询。
   压缩格式：LZO。
   关键表：
   dim_sku_info：商品维度表。这是本项目的核心维度表，通过将ods_sku_info与类目、库存等信息关联，丰富了商品的属性，为后续所有分析提供了强大的支撑。
3. DWD层
   作用：对ODS层的数据进行清洗、去重、标准化、维度退化，构建出干净、统一、原子粒度的事实表。这是数据仓库的“黄金层”，为上层分析提供一致、可信的数据基础。
   技术实现：
   存储格式：PARQUET。
   压缩格式：LZO。
   关键表：
   事实表：
   dwd_order_info：清洗后的订单事实表，处理了订单状态、金额等逻辑。
   dwd_order_detail：清洗后的订单明细事实表，将订单与商品、优惠等信息关联。
   dwd_payment_info：支付事实表，与订单事实表关联。
   dwd_comment_info：评价事实表。
   dwd_page_log：清洗后的用户行为日志事实表，标准化了访问来源、搜索词等字段。
   维度表：
   dim_sku_info：商品维度表（在DWD层创建，扮演DIM层角色）。
4. DWS层
   作用：预聚合，将DWD层的业务事实按“维度+度量”进行轻度汇总，减少指标的重复计算，提高代码复用性，保留下钻能力，减少数据量，从而显著提高查询性能。
   技术实现：
   存储格式：PARQUET。
   压缩格式：LZO。
   关键表 (面向品类360看板的主题)：
   dws_store_category_daycount：每日店铺品类表。按store_id和category3_id聚合了访客数、支付买家数、支付件数、支付转化率、支付金额等核心销售指标。
   dws_attr_distribution_analysis：属性分布分析表。按品类和商品属性（如颜色、尺码）聚合了支付金额、支付件数、转化率等指标。
   dws_store_source_daycount：每日店铺来源表。按店铺和流量来源聚合了访客、加购、收藏、支付等数据。
   dws_store_user_daycount, dws_store_age_daycount, dws_store_gender_daycount：每日店铺用户画像表。按店铺维度聚合了新老客、性别、年龄等不同人群的支付数据。
   dws_keyword_traffic_analysis：关键词引流分析表。按店铺和搜索词聚合了访客量。
5. ADS层
   作用：面向“品类360看板”这一具体的应用场景，将DWS层的多个汇总表进行JOIN，计算出最终的、可直接用于前端BI工具（如生意参谋）展示的宽表。这是数据价值的最终出口。
   技术实现：
   存储格式：PARQUET。
   压缩格式：LZO。


实现方式：
分层实现 (Recommended)：
优点：逻辑清晰，各层职责分明，数据可复用，易于维护和排查问题。
过程：ODS -> DWD -> DWS -> ADS。ADS层的SQL通过JOIN dws_store_category_daycount, dws_attr_distribution_analysis, dws_store_source_daycount 等DWS表来构建最终结果。
不分层实现：
优点：开发速度快，代码集中。
缺点：逻辑复杂，难以维护，性能可能不佳，数据不可复用。
过程：直接从DWD层的dwd_order_detail, dwd_page_log等事实表，以及dim_sku_info维度表，进行复杂的JOIN和GROUP BY，在一个SQL中完成所有计算。


三、 关键指标与功能实现
根据项目工单，我们设计并实现了多个关键指标，全面覆盖了看板的各个功能模块。

1. 销售分析
   核心概况：
   指标：payment_amount（支付金额/GMV）、payment_num（支付件数）、visitor_count（访客数）、pay_rate（支付转化率）。
   来源：dws_store_category_daycount。

商品排名：
指标：各商品的store_split_final_amount（支付金额）。
来源：dws_store_category_daycount，按category3_id筛选，按store_split_final_amount降序排列。

月支付金额进度：
指标：current_month_gmv / target_month_gmv。
来源：dws_store_category_daycount (当前GMV) + 一个独立的“运营目标表” (目标GMV)。

月支付金额贡献：
指标：category_gmv / total_store_gmv。
来源：dws_store_category_daycount 中特定品类的GMV除以全店所有品类的GMV总和。

上月本店支付金额排名：
指标：rank() over (partition by store_id order by payment_amount desc)。
来源：dws_store_category_daycount，按店铺分组，对所有品类的支付金额进行排名。

2. 属性分析
   指标：按category3_id和attr_value_name（属性值，如“红色”、“L码”）分组的visitor_count（流量）、payment_user_count（支付买家数）、conversion_rate（转化率）、action_num（动销商品数）。
   来源：dws_attr_distribution_analysis。此表通过复杂的FULL OUTER JOIN，将商品的属性信息与销售、流量数据关联起来。
3. 流量分析
   指标：各source_type（流量来源）的visitor_count（访客占比）、payment_user_count（支付买家数）、conversion_rate（支付转化率）。
   来源：dws_store_source_daycount。
   热搜词分析：
   指标：search_word（搜索词）、visitor_count（访客量）。
   来源：dws_keyword_traffic_analysis，通过解析dwd_page_log中的搜索行为得到。
4. 客群洞察
   指标：访问该品类的搜索/访问/支付人群的gender_ratio（性别分布）、age_group_ratio（年龄分布）、new_user_ratio（新客占比）。
   来源：dws_store_gender_daycount, dws_store_age_daycount, dws_store_user_daycount。通过分析这些表，可以了解不同行为阶段的人群画像。





