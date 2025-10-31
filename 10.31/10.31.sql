use bigdata_realtime_lululemon_report_v1;


select  *  from report_lululemon_window_gmv_topN limit 100;


SELECT
    ds,
    window_end ,
    win_gmv AS win_gmv,
    top5_product_ids AS top5_product_ids
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
         FROM report_lululemon_window_gmv_topN
     ) t
WHERE rn = 1
ORDER BY ds DESC;






-- 列转行
# select
#     ds,
#     window_end,
#     win_gmv,
#     t1.exploded_product_id,
#     t2.product_desc
#     from
#         (SELECT
#                       ds,
#                       window_end,
#                       win_gmv,
#                       exploded_product_id
#                FROM (SELECT ds,
#                             window_end,
#                             win_gmv,
#                             top5_product_ids,
#                             ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
#                      FROM report_lululemon_window_gmv_topN) t LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
#                WHERE rn = 1 and exploded_product_id !='数据缺失'
#                ORDER BY ds DESC
#                 )t1  left join biggta_realtime_report_v3.spider_lululemon_jd_product_dtl t2
#                     on t1.exploded_product_id = t2.product_id;




# SELECT
#     t1.ds,
#     t1.window_end,
#     t1.win_gmv,
#     t1.exploded_product_id,
#     t2.product_desc
# FROM (
#          SELECT
#              ds,
#              window_end,
#              win_gmv,
#              exploded_product_id
#          FROM (
#                   SELECT
#                       ds,
#                       window_end,
#                       win_gmv,
#                       top5_product_ids,
#                       ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
#                   FROM report_lululemon_window_gmv_topN
#               ) t
#              LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
#          WHERE rn = 1
#            AND exploded_product_id != '数据缺失'
#      ) t1
#          LEFT JOIN biggta_realtime_report_v3.spider_lululemon_jd_product_dtl t2
#                    ON t1.exploded_product_id = t2.product_id
# ORDER BY t1.ds ASC;


#     concat( round(((a.win_gmv - b.win_gmv) / b.win_gmv) * 100 ,2) ,'%') as Mon_Rate
#     a.window_start,
# concat(round(((a.win_gmv - b.win_gmv) / b.win_gmv) * 100, 2),'%') AS Mon_Rate



select
    b.ds,
    b.window_start,
    b.window_end,
    b.win_gmv,
    b.top5_product_descs,
    CASE
        WHEN a.win_gmv IS NULL OR a.win_gmv = 0 THEN NULL
        ELSE concat(round(((b.win_gmv - a.win_gmv) / a.win_gmv) * 100, 2), '%')
        END AS Mon_Rate
    from
        (SELECT t1.ds,
                      t1.window_start,
                      t1.window_end,
                      t1.win_gmv,
                      group_concat(t2.product_desc, ',') AS top5_product_descs
               FROM (SELECT ds,
                            window_end,
                            window_start,
                            win_gmv,
                            exploded_product_id
                     FROM (SELECT ds,
                                  window_end,
                                  win_gmv,
                                  window_start,
                                  top5_product_ids,
                                  ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                           FROM report_lululemon_window_gmv_topN) t LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
                     WHERE rn = 1
                       AND exploded_product_id != '数据缺失') t1
                        LEFT JOIN biggta_realtime_report_v3.spider_lululemon_jd_product_dtl t2
                                  ON t1.exploded_product_id = t2.product_id
               GROUP BY t1.ds,
                        t1.window_end,
                        t1.window_start,
                        t1.win_gmv
               ) a
        right  join (SELECT t1.ds,
                      t1.window_start,
                      t1.window_end,
                      t1.win_gmv,
                      group_concat(t2.product_desc, ',') AS top5_product_descs
               FROM (SELECT ds,
                            window_end,
                            window_start,
                            win_gmv,
                            exploded_product_id
                     FROM (SELECT ds,
                                  window_end,
                                  win_gmv,
                                  window_start,
                                  top5_product_ids,
                                  ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                           FROM report_lululemon_window_gmv_topN) t LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
                     WHERE rn = 1
                       AND exploded_product_id != '数据缺失') t1
                        LEFT JOIN biggta_realtime_report_v3.spider_lululemon_jd_product_dtl t2
                                  ON t1.exploded_product_id = t2.product_id
               GROUP BY t1.ds,
                        t1.window_end,
                        t1.window_start,
                        t1.win_gmv
) b
         on datediff(
             date_format(str_to_date(b.window_start, '%Y-%m-%d'), '%Y-%m-%d') ,
             date_format(str_to_date(a.window_start, '%Y-%m-%d'), '%Y-%m-%d')) = 1
order by b.ds;







-- 已经可以出环比了

WITH base AS (
    SELECT
        ds,
        window_start,
        window_end,
        win_gmv,
        group_concat(t2.product_desc, ',') AS top5_product_descs
    FROM (
             SELECT
                 ds,
                 window_end,
                 window_start,
                 win_gmv,
                 exploded_product_id
             FROM (
                      SELECT
                          ds,
                          window_end,
                          win_gmv,
                          window_start,
                          top5_product_ids,
                          ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS Mon_Rate
                      FROM report_lululemon_window_gmv_topN
                  ) t
                 LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
             WHERE rn = 1
               AND exploded_product_id != '数据缺失'
         ) t1
             LEFT JOIN biggta_realtime_report_v3.spider_lululemon_jd_product_dtl t2
                       ON t1.exploded_product_id = t2.product_id
    GROUP BY t1.ds, t1.window_end, t1.window_start, t1.win_gmv
),

-- 当天数据
     b AS (
         SELECT * FROM base
     ),

-- 前一天数据
     a AS (
         SELECT * FROM base
     )

SELECT
    b.ds,
    b.window_start,
    b.win_gmv,
    b.top5_product_descs,
    CASE
        WHEN a.win_gmv IS NULL OR a.win_gmv = 0 THEN NULL
        ELSE concat(round(((b.win_gmv - a.win_gmv) / a.win_gmv) * 100, 2), '%')
        END AS rm
FROM b
         LEFT JOIN a
                   ON datediff(
                              date_format(str_to_date(b.window_start, '%Y-%m-%d'), '%Y-%m-%d'),
                              date_format(str_to_date(a.window_start, '%Y-%m-%d'), '%Y-%m-%d')
                          ) = 1
ORDER BY b.ds;


-- 重复率



#     a.top5_product_ids AS last_day_product_ids,  -- ✅ 新增 昨天商品ID数组
# a.win_gmv AS last_day_win_gmv                -- ✅ 可选：方便对照


SELECT
    b.ds,
    b.window_start,
    b.window_end,
    b.win_gmv,
    b.top5_product_ids,
    a.top5_product_ids AS last_day_product_ids,
    b.top5_product_descs,
    -- 环比增长率
    CASE
        WHEN a.win_gmv IS NULL OR a.win_gmv = 0 THEN NULL
        ELSE concat(round(((b.win_gmv - a.win_gmv) / a.win_gmv) * 100, 2), '%')
        END AS Mon_Rate,
    -- 商品重复率（修正版）
    CASE
        WHEN inter.intersect_cnt IS NULL THEN NULL
        ELSE concat(round((inter.intersect_cnt / 5.0) * 100, 2), '%')
        END AS repeat_rate
FROM
    (
        -- ===== 当天数据 =====
        SELECT
            t1.ds,
            t1.window_start,
            t1.window_end,
            t1.win_gmv,
            group_concat(t2.product_desc, ',') AS top5_product_descs,
            group_concat(t1.exploded_product_id, ',') AS top5_product_ids
        FROM (
                 SELECT
                     ds,
                     window_start,
                     window_end,
                     win_gmv,
                     exploded_product_id
                 FROM (
                          SELECT
                              ds,
                              window_start,
                              window_end,
                              win_gmv,
                              top5_product_ids,
                              ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                          FROM report_lululemon_window_gmv_topN
                      ) t
                     LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
                 WHERE rn = 1 AND exploded_product_id != '数据缺失'
             ) t1
                 LEFT JOIN biggta_realtime_report_v3.spider_lululemon_jd_product_dtl t2
                           ON t1.exploded_product_id = t2.product_id
        GROUP BY t1.ds, t1.window_start, t1.window_end, t1.win_gmv
    ) b
        LEFT JOIN
    (
        -- ===== 前一天数据 =====
        SELECT
            t1.ds,
            t1.window_start,
            t1.window_end,
            t1.win_gmv,
            group_concat(t1.exploded_product_id, ',') AS top5_product_ids
        FROM (
                 SELECT
                     ds,
                     window_start,
                     window_end,
                     win_gmv,
                     exploded_product_id
                 FROM (
                          SELECT
                              ds,
                              window_start,
                              window_end,
                              win_gmv,
                              top5_product_ids,
                              ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                          FROM report_lululemon_window_gmv_topN
                      ) t
                     LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
                 WHERE rn = 1 AND exploded_product_id != '数据缺失'
             ) t1
        GROUP BY t1.ds, t1.window_start, t1.window_end, t1.win_gmv
    ) a
    ON datediff(b.ds, a.ds) = 1
        LEFT JOIN
    (
        -- ===== 修正版交集计算 =====
        SELECT
            b.ds AS ds,
            COUNT(DISTINCT b.product_id) AS intersect_cnt
        FROM (
                 SELECT
                     ds,
                     exploded_product_id AS product_id
                 FROM (
                          SELECT
                              ds,
                              top5_product_ids,
                              ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                          FROM report_lululemon_window_gmv_topN
                      ) t
                     LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
                 WHERE rn = 1
             ) b
                 JOIN (
            SELECT
                ds,
                exploded_product_id AS product_id
            FROM (
                     SELECT
                         ds,
                         top5_product_ids,
                         ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                     FROM report_lululemon_window_gmv_topN
                 ) t
                LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
            WHERE rn = 1
        ) a
                      ON b.product_id = a.product_id
                          AND datediff(b.ds, a.ds) = 1
        GROUP BY b.ds
    ) inter
    ON inter.ds = b.ds
ORDER BY b.ds;




SELECT
    b.ds,
    b.window_start,
    b.window_end,
    b.win_gmv,
    b.top5_product_ids,
    a.top5_product_ids AS last_day_product_ids,
    b.top5_product_descs,
    a.top5_product_descs AS last_day_product_descs,  -- ✅ 新增：昨天Top5商品名称
    -- 环比增长率
    CASE
        WHEN a.win_gmv IS NULL OR a.win_gmv = 0 THEN NULL
        ELSE concat(round(((b.win_gmv - a.win_gmv) / a.win_gmv) * 100, 2), '%')
        END AS Mon_Rate,
    -- 商品重复率
    CASE
        WHEN inter.intersect_cnt IS NULL THEN NULL
        ELSE concat(round((inter.intersect_cnt / 5.0) * 100, 2), '%')
        END AS repeat_rate
FROM
    (
        -- ===== 当天数据 =====
        SELECT
            t1.ds,
            t1.window_start,
            t1.window_end,
            t1.win_gmv,
            group_concat(t2.product_desc, ',') AS top5_product_descs,
            group_concat(t1.exploded_product_id, ',') AS top5_product_ids
        FROM (
                 SELECT
                     ds,
                     window_start,
                     window_end,
                     win_gmv,
                     exploded_product_id
                 FROM (
                          SELECT
                              ds,
                              window_start,
                              window_end,
                              win_gmv,
                              top5_product_ids,
                              ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                          FROM report_lululemon_window_gmv_topN
                      ) t
                     LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
                 WHERE rn = 1 AND exploded_product_id != '数据缺失'
             ) t1
                 LEFT JOIN biggta_realtime_report_v3.spider_lululemon_jd_product_dtl t2
                           ON t1.exploded_product_id = t2.product_id
        GROUP BY t1.ds, t1.window_start, t1.window_end, t1.win_gmv
    ) b
        LEFT JOIN
    (
        -- ===== 前一天数据（含名称） =====
        SELECT
            t1.ds,
            t1.window_start,
            t1.window_end,
            t1.win_gmv,
            group_concat(t2.product_desc, ',') AS top5_product_descs,     -- ✅ 新增商品名称
            group_concat(t1.exploded_product_id, ',') AS top5_product_ids
        FROM (
                 SELECT
                     ds,
                     window_start,
                     window_end,
                     win_gmv,
                     exploded_product_id
                 FROM (
                          SELECT
                              ds,
                              window_start,
                              window_end,
                              win_gmv,
                              top5_product_ids,
                              ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                          FROM report_lululemon_window_gmv_topN
                      ) t
                     LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
                 WHERE rn = 1 AND exploded_product_id != '数据缺失'
             ) t1
                 LEFT JOIN biggta_realtime_report_v3.spider_lululemon_jd_product_dtl t2
                           ON t1.exploded_product_id = t2.product_id
        GROUP BY t1.ds, t1.window_start, t1.window_end, t1.win_gmv
    ) a
    ON datediff(b.ds, a.ds) = 1
        LEFT JOIN
    (
        -- ===== 修正版交集计算 =====
        SELECT
            b.ds AS ds,
            COUNT(DISTINCT b.product_id) AS intersect_cnt
        FROM (
                 SELECT
                     ds,
                     exploded_product_id AS product_id
                 FROM (
                          SELECT
                              ds,
                              top5_product_ids,
                              ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                          FROM report_lululemon_window_gmv_topN
                      ) t
                     LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
                 WHERE rn = 1
             ) b
                 JOIN (
            SELECT
                ds,
                exploded_product_id AS product_id
            FROM (
                     SELECT
                         ds,
                         top5_product_ids,
                         ROW_NUMBER() OVER (PARTITION BY ds ORDER BY window_end DESC) AS rn
                     FROM report_lululemon_window_gmv_topN
                 ) t
                LATERAL VIEW explode_split(top5_product_ids, ',') tmp AS exploded_product_id
            WHERE rn = 1
        ) a
                      ON b.product_id = a.product_id
                          AND datediff(b.ds, a.ds) = 1
        GROUP BY b.ds
    ) inter
    ON inter.ds = b.ds
ORDER BY b.ds;



select * from logs_user_info_message;









