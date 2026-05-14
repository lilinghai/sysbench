-- 这段 SQL 涉及的是 Atlassian Jira Service Management (JSM) 中的 Assets（前身为 Insight） 模块。
-- 在 Assets 中，数据是以“对象（Object）”和“关系（Relationship）”的形式存储的。
-- obj_new (别名 o 和 o1): 这是 Assets 中存储对象实例的核心表。它存储了资产的具体信息，如服务器、笔记本电脑、员工、供应商等。
-- obj_relationship_new (别名 r): 这是关联关系表。它定义了两个对象之间的逻辑连接（例如：“服务器 A” 运行在 “宿主机 B” 上）。
-- 这个 SQL 的执行意图可以描述为：
-- 在指定的工作空间中，找出所有与西门子（Siemens）相关资产有关联的资产列表。
-- SQL 通过 r.referenced_object_id = o.id 和 r.object_id = o1.id 建立了从 o1 指向 o 的关联。
-- o1 (源对象): 满足特定过滤条件（厂商为 Siemens）的发起对象。
-- o (目标对象): 我们最终想要查询并显示的关联资产。
-- workspace_id = 'aa01e3d3-0423-4614-8004-206989601265' 这表明该查询被严格锁定在某个特定的 Assets 租户环境内。
-- obj_type_id IN (...) 中包含了 5 个特定的 UUID。在业务上，这意味着查询仅限于特定的资产类别（例如：可能是“服务器”、“路由器”、“防火墙”、“软件授权”等）。
-- sql1 match Siemens
SELECT
    o.sequential_id,
    o.label
FROM
    obj_new o
    INNER JOIN obj_relationship_new r ON r.referenced_object_id = o.id
    INNER JOIN obj_new o1 ON o1.id = r.object_id
WHERE
    o.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o1.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND o1.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND MATCH (
        o1.workspace_id,
        o1.text_value_1,
        o1.text_value_2,
        o1.text_value_3,
        o1.text_value_4,
        o1.text_value_5,
        o1.text_value_6,
        o1.text_value_7,
        o1.text_value_8,
        o1.text_value_9,
        o1.text_value_10,
        o1.text_value_11,
        o1.text_value_12,
        o1.text_value_13,
        o1.text_value_14,
        o1.text_value_15
    ) AGAINST ('Siemens' IN BOOLEAN MODE)
ORDER BY
    o.label ASC
LIMIT
    1000
OFFSET
    0;

-- sql2 match both UUID and Siemens
SELECT
    o.sequential_id,
    o.label
FROM
    obj_new o
    INNER JOIN obj_relationship_new r ON r.referenced_object_id = o.id
    INNER JOIN obj_new o1 ON o1.id = r.object_id
WHERE
    o.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o1.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND o1.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    and MATCH (
        o1.workspace_id,
        o1.text_value_1,
        o1.text_value_2,
        o1.text_value_3,
        o1.text_value_4,
        o1.text_value_5,
        o1.text_value_6,
        o1.text_value_7,
        o1.text_value_8,
        o1.text_value_9,
        o1.text_value_10,
        o1.text_value_11,
        o1.text_value_12,
        o1.text_value_13,
        o1.text_value_14,
        o1.text_value_15
    ) AGAINST (
        '+"aa01e3d3-0423-4614-8004-206989601265" +Siemens' IN BOOLEAN MODE
    )
ORDER BY
    o.label ASC
LIMIT
    1000
OFFSET
    0;

-- sql3 match Siemens and not match China
SELECT
    o.sequential_id,
    o.label
FROM
    obj_new o
    INNER JOIN obj_relationship_new r ON r.referenced_object_id = o.id
    INNER JOIN obj_new o1 ON o1.id = r.object_id
WHERE
    o.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o1.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND o1.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND MATCH (
        o1.workspace_id,
        o1.text_value_1,
        o1.text_value_2,
        o1.text_value_3,
        o1.text_value_4,
        o1.text_value_5,
        o1.text_value_6,
        o1.text_value_7,
        o1.text_value_8,
        o1.text_value_9,
        o1.text_value_10,
        o1.text_value_11,
        o1.text_value_12,
        o1.text_value_13,
        o1.text_value_14,
        o1.text_value_15
    ) AGAINST ('+Siemens -China' IN BOOLEAN MODE)
ORDER BY
    o.label ASC
LIMIT
    1000
OFFSET
    0;

-- sql4 match phrase "US Siemens"
SELECT
    o.sequential_id,
    o.label
FROM
    obj_new o
    INNER JOIN obj_relationship_new r ON r.referenced_object_id = o.id
    INNER JOIN obj_new o1 ON o1.id = r.object_id
WHERE
    o.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o1.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND o1.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND MATCH (
        o1.workspace_id,
        o1.text_value_1,
        o1.text_value_2,
        o1.text_value_3,
        o1.text_value_4,
        o1.text_value_5,
        o1.text_value_6,
        o1.text_value_7,
        o1.text_value_8,
        o1.text_value_9,
        o1.text_value_10,
        o1.text_value_11,
        o1.text_value_12,
        o1.text_value_13,
        o1.text_value_14,
        o1.text_value_15
    ) AGAINST ('"US Siemens"' IN BOOLEAN MODE)
ORDER BY
    o.label ASC
LIMIT
    1000
OFFSET
    0;


-- sql5 match word with "apple" prefix and "Chine" word
SELECT
    o.sequential_id,
    o.label
FROM
    obj_new o
    INNER JOIN obj_relationship_new r ON r.referenced_object_id = o.id
    INNER JOIN obj_new o1 ON o1.id = r.object_id
WHERE
    o.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o1.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND o1.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND MATCH (
        o1.workspace_id,
        o1.text_value_1,
        o1.text_value_2,
        o1.text_value_3,
        o1.text_value_4,
        o1.text_value_5,
        o1.text_value_6,
        o1.text_value_7,
        o1.text_value_8,
        o1.text_value_9,
        o1.text_value_10,
        o1.text_value_11,
        o1.text_value_12,
        o1.text_value_13,
        o1.text_value_14,
        o1.text_value_15
    ) AGAINST ('+apple* +Chine' IN BOOLEAN MODE)
ORDER BY
    o.label ASC
LIMIT
    1000
OFFSET
    0;

SELECT
    o.sequential_id,
    o.label
FROM
    obj_new o
    INNER JOIN obj_relationship_new r ON r.referenced_object_id = o.id
    INNER JOIN obj_new o1 ON o1.id = r.object_id
WHERE
    o.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o1.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
    AND o.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND o1.obj_type_id IN (
        UUID_TO_BIN ('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
        UUID_TO_BIN ('3307bd5f-0564-4aea-807f-10b71c936cb8'),
        UUID_TO_BIN ('770e0734-c440-47b0-90de-6abd76ec9fe2'),
        UUID_TO_BIN ('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
        UUID_TO_BIN ('b95ee1c2-f117-4f9c-8dd7-0473a70d3237')
    )
    AND MATCH (
        o1.workspace_id,
        o1.text_value_1,
        o1.text_value_2,
        o1.text_value_3,
        o1.text_value_4,
        o1.text_value_5,
        o1.text_value_6,
        o1.text_value_7,
        o1.text_value_8,
        o1.text_value_9,
        o1.text_value_10,
        o1.text_value_11,
        o1.text_value_12,
        o1.text_value_13,
        o1.text_value_14,
        o1.text_value_15
    ) AGAINST ('-Siemens' IN BOOLEAN MODE)
ORDER BY
    o.label ASC
LIMIT
    1000
OFFSET
    0;