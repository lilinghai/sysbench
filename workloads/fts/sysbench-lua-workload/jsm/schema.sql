create database jsm_assets;
create table jsm_assets.obj_new
(
    id                          binary(16)                           not null
        primary key,
    workspace_id                varchar(255)                         not null,
    sequential_id               bigint                               null,
    label                       varchar(255)                         null,
    obj_type_id                 binary(16)                           not null,
    schema_id                   binary(16)                           not null,
    other_values                json                                 null,
    other_values_indexed        json                                 null,
    external_id                 text                                 null,
    text_value_1                text                                 null,
    text_value_2                text                                 null,
    text_value_3                text                                 null,
    text_value_4                text                                 null,
    text_value_5                text                                 null,
    text_value_6                text                                 null,
    text_value_7                text                                 null,
    text_value_8                text                                 null,
    text_value_9                text                                 null,
    text_value_10               text                                 null,
    text_value_12               text                                 null,
    text_value_13               text                                 null,
    text_value_14               text                                 null,
    text_value_15               text                                 null,
    text_value_16               text                                 null,
    text_value_17               text                                 null,
    text_value_18               text                                 null,
    text_value_19               text                                 null,
    text_value_20               text                                 null,
    text_value_11               text                                 null,
    text_value_21               text                                 null,
    text_value_22               text                                 null,
    text_value_23               text                                 null,
    text_value_24               text                                 null,
    text_value_25               text                                 null,
    text_value_26               text                                 null,
    text_value_27               text                                 null,
    text_value_28               text                                 null,
    text_value_29               text                                 null,
    text_value_30               text                                 null,
    text_value_31               text                                 null,
    text_value_32               text                                 null,
    text_value_33               text                                 null,
    text_value_34               text                                 null,
    text_value_35               text                                 null,
    numeric_value_1             decimal(65, 30)                      null,
    numeric_value_2             decimal(65, 30)                      null,
    numeric_value_3             decimal(65, 30)                      null,
    numeric_value_4             decimal(65, 30)                      null,
    numeric_value_5             decimal(65, 30)                      null,
    numeric_value_6             decimal(65, 30)                      null,
    numeric_value_7             decimal(65, 30)                      null,
    numeric_value_8             decimal(65, 30)                      null,
    numeric_value_9             decimal(65, 30)                      null,
    numeric_value_10            decimal(65, 30)                      null,
    numeric_value_11            decimal(65, 30)                      null,
    numeric_value_12            decimal(65, 30)                      null,
    numeric_value_13            decimal(65, 30)                      null,
    numeric_value_14            decimal(65, 30)                      null,
    numeric_value_15            decimal(65, 30)                      null,
    numeric_value_16            decimal(65, 30)                      null,
    numeric_value_17            decimal(65, 30)                      null,
    numeric_value_18            decimal(65, 30)                      null,
    numeric_value_19            decimal(65, 30)                      null,
    numeric_value_20            decimal(65, 30)                      null,
    text_value_36               text                                 null,
    text_value_37               text                                 null,
    text_value_38               text                                 null,
    text_value_39               text                                 null,
    text_value_40               text                                 null,
    text_value_41               text                                 null,
    text_value_42               text                                 null,
    text_value_43               text                                 null,
    text_value_44               text                                 null,
    text_value_45               text                                 null,
    text_value_46               text                                 null,
    text_value_47               text                                 null,
    text_value_48               text                                 null,
    text_value_49               text                                 null,
    text_value_50               text                                 null,
    text_value_51               text                                 null,
    text_value_52               text                                 null,
    text_value_53               text                                 null,
    text_value_54               text                                 null,
    text_value_55               text                                 null,
    text_value_56               text                                 null,
    text_value_57               text                                 null,
    text_value_58               text                                 null,
    text_value_59               text                                 null,
    text_value_60               text                                 null,
    text_value_61               text                                 null,
    text_value_62               text                                 null,
    text_value_63               text                                 null,
    text_value_64               text                                 null,
    text_value_65               text                                 null,
    text_value_66               text                                 null,
    text_value_67               text                                 null,
    text_value_68               text                                 null,
    text_value_69               text                                 null,
    text_value_70               text                                 null,
    text_value_71               text                                 null,
    text_value_72               text                                 null,
    text_value_73               text                                 null,
    text_value_74               text                                 null,
    text_value_75               text                                 null,
    text_value_76               text                                 null,
    text_value_77               text                                 null,
    text_value_78               text                                 null,
    text_value_79               text                                 null,
    text_value_80               text                                 null,
    text_value_81               text                                 null,
    text_value_82               text                                 null,
    text_value_83               text                                 null,
    text_value_84               text                                 null,
    text_value_85               text                                 null,
    text_value_86               text                                 null,
    text_value_87               text                                 null,
    text_value_88               text                                 null,
    text_value_89               text                                 null,
    text_value_90               text                                 null,
    text_value_91               text                                 null,
    text_value_92               text                                 null,
    text_value_93               text                                 null,
    text_value_94               text                                 null,
    text_value_95               text                                 null,
    text_value_96               text                                 null,
    text_value_97               text                                 null,
    text_value_98               text                                 null,
    text_value_99               text                                 null,
    text_value_100              text                                 null,
    text_value_101              text                                 null,
    text_value_102              text                                 null,
    text_value_103              text                                 null,
    text_value_104              text                                 null,
    text_value_105              text                                 null,
    text_value_106              text                                 null,
    text_value_107              text                                 null,
    text_value_108              text                                 null,
    text_value_109              text                                 null,
    text_value_110              text                                 null,
    text_value_111              text                                 null,
    text_value_112              text                                 null,
    text_value_113              text                                 null,
    text_value_114              text                                 null,
    text_value_115              text                                 null,
    text_value_116              text                                 null,
    text_value_117              text                                 null,
    text_value_118              text                                 null,
    text_value_119              text                                 null,
    text_value_120              text                                 null,
    text_value_121              text                                 null,
    text_value_122              text                                 null,
    text_value_123              text                                 null,
    text_value_124              text                                 null,
    text_value_125              text                                 null,
    text_value_126              text                                 null,
    text_value_127              text                                 null,
    text_value_128              text                                 null,
    text_value_129              text                                 null,
    text_value_130              text                                 null,
    text_value_131              text                                 null,
    text_value_132              text                                 null,
    text_value_133              text                                 null,
    text_value_134              text                                 null,
    text_value_135              text                                 null,
    text_value_136              text                                 null,
    text_value_137              text                                 null,
    text_value_138              text                                 null,
    text_value_139              text                                 null,
    text_value_140              text                                 null,
    text_value_141              text                                 null,
    text_value_142              text                                 null,
    text_value_143              text                                 null,
    text_value_144              text                                 null,
    text_value_145              text                                 null,
    text_value_146              text                                 null,
    text_value_147              text                                 null,
    text_value_148              text                                 null,
    text_value_149              text                                 null,
    text_value_150              text                                 null,
    text_value_151              text                                 null,
    text_value_152              text                                 null,
    text_value_153              text                                 null,
    text_value_154              text                                 null,
    text_value_155              text                                 null,
    numeric_value_21            decimal(65, 17)                      null,
    numeric_value_22            decimal(65, 17)                      null,
    numeric_value_23            decimal(65, 17)                      null,
    numeric_value_24            decimal(65, 17)                      null,
    numeric_value_25            decimal(65, 17)                      null,
    numeric_value_26            decimal(65, 17)                      null,
    numeric_value_27            decimal(65, 17)                      null,
    numeric_value_28            decimal(65, 17)                      null,
    numeric_value_29            decimal(65, 17)                      null,
    numeric_value_30            decimal(65, 17)                      null,
    numeric_value_31            decimal(65, 17)                      null,
    numeric_value_32            decimal(65, 17)                      null,
    numeric_value_33            decimal(65, 17)                      null,
    numeric_value_34            decimal(65, 17)                      null,
    numeric_value_35            decimal(65, 17)                      null,
    numeric_value_36            decimal(65, 17)                      null,
    numeric_value_37            decimal(65, 17)                      null,
    numeric_value_38            decimal(65, 17)                      null,
    numeric_value_39            decimal(65, 17)                      null,
    numeric_value_40            decimal(65, 17)                      null,
    numeric_value_41            decimal(65, 17)                      null,
    numeric_value_42            decimal(65, 17)                      null,
    numeric_value_43            decimal(65, 17)                      null,
    numeric_value_44            decimal(65, 17)                      null,
    numeric_value_45            decimal(65, 17)                      null,
    numeric_value_46            decimal(65, 17)                      null,
    numeric_value_47            decimal(65, 17)                      null,
    numeric_value_48            decimal(65, 17)                      null,
    numeric_value_49            decimal(65, 17)                      null,
    numeric_value_50            decimal(65, 17)                      null,
    numeric_value_51            decimal(65, 17)                      null,
    numeric_value_52            decimal(65, 17)                      null,
    numeric_value_53            decimal(65, 17)                      null,
    numeric_value_54            decimal(65, 17)                      null,
    numeric_value_55            decimal(65, 17)                      null,
    numeric_value_56            decimal(65, 17)                      null,
    numeric_value_57            decimal(65, 17)                      null,
    numeric_value_58            decimal(65, 17)                      null,
    numeric_value_59            decimal(65, 17)                      null,
    numeric_value_60            decimal(65, 17)                      null,
    numeric_value_61            decimal(65, 17)                      null,
    numeric_value_62            decimal(65, 17)                      null,
    numeric_value_63            decimal(65, 17)                      null,
    numeric_value_64            decimal(65, 17)                      null,
    numeric_value_65            decimal(65, 17)                      null,
    numeric_value_66            decimal(65, 17)                      null,
    numeric_value_67            decimal(65, 17)                      null,
    numeric_value_68            decimal(65, 17)                      null,
    numeric_value_69            decimal(65, 17)                      null,
    numeric_value_70            decimal(65, 17)                      null,
    numeric_value_71            decimal(65, 17)                      null,
    numeric_value_72            decimal(65, 17)                      null,
    numeric_value_73            decimal(65, 17)                      null,
    numeric_value_74            decimal(65, 17)                      null,
    numeric_value_75            decimal(65, 17)                      null,
    numeric_value_76            decimal(65, 17)                      null,
    numeric_value_77            decimal(65, 17)                      null,
    numeric_value_78            decimal(65, 17)                      null,
    numeric_value_79            decimal(65, 17)                      null,
    numeric_value_80            decimal(65, 17)                      null,
    numeric_value_81            decimal(65, 17)                      null,
    numeric_value_82            decimal(65, 17)                      null,
    numeric_value_83            decimal(65, 17)                      null,
    numeric_value_84            decimal(65, 17)                      null,
    numeric_value_85            decimal(65, 17)                      null,
    numeric_value_86            decimal(65, 17)                      null,
    numeric_value_87            decimal(65, 17)                      null,
    numeric_value_88            decimal(65, 17)                      null,
    numeric_value_89            decimal(65, 17)                      null,
    numeric_value_90            decimal(65, 17)                      null,
    numeric_value_91            decimal(65, 17)                      null,
    numeric_value_92            decimal(65, 17)                      null,
    numeric_value_93            decimal(65, 17)                      null,
    numeric_value_94            decimal(65, 17)                      null,
    numeric_value_95            decimal(65, 17)                      null,
    numeric_value_96            decimal(65, 17)                      null,
    numeric_value_97            decimal(65, 17)                      null,
    numeric_value_98            decimal(65, 17)                      null,
    numeric_value_99            decimal(65, 17)                      null,
    numeric_value_100           decimal(65, 17)                      null,
    numeric_value_101           decimal(65, 17)                      null,
    numeric_value_102           decimal(65, 17)                      null,
    numeric_value_103           decimal(65, 17)                      null,
    numeric_value_104           decimal(65, 17)                      null,
    numeric_value_105           decimal(65, 17)                      null,
    numeric_value_106           decimal(65, 17)                      null,
    numeric_value_107           decimal(65, 17)                      null,
    numeric_value_108           decimal(65, 17)                      null,
    numeric_value_109           decimal(65, 17)                      null,
    numeric_value_110           decimal(65, 17)                      null,
    numeric_value_111           decimal(65, 17)                      null,
    numeric_value_112           decimal(65, 17)                      null,
    numeric_value_113           decimal(65, 17)                      null,
    numeric_value_114           decimal(65, 17)                      null,
    numeric_value_115           decimal(65, 17)                      null,
    numeric_value_116           decimal(65, 17)                      null,
    numeric_VALUE_117           decimal(65, 17)                      null,
    numeric_value_118           decimal(65, 17)                      null,
    numeric_value_119           decimal(65, 17)                      null,
    numeric_value_120           decimal(65, 17)                      null,
    numeric_value_121           decimal(65, 17)                      null,
    numeric_value_122           decimal(65, 17)                      null,
    numeric_value_123           decimal(65, 17)                      null,
    numeric_value_124           decimal(65, 17)                      null,
    numeric_value_125           decimal(65, 17)                      null,
    numeric_value_126           decimal(65, 17)                      null,
    numeric_value_127           decimal(65, 17)                      null,
    numeric_value_128           decimal(65, 17)                      null,
    numeric_value_129           decimal(65, 17)                      null,
    numeric_value_130           decimal(65, 17)                      null,
    numeric_value_131           decimal(65, 17)                      null,
    numeric_value_132           decimal(65, 17)                      null,
    numeric_value_133           decimal(65, 17)                      null,
    numeric_value_134           decimal(65, 17)                      null,
    numeric_value_135           decimal(65, 17)                      null,
    numeric_value_136           decimal(65, 17)                      null,
    numeric_value_137           decimal(65, 17)                      null,
    numeric_value_138           decimal(65, 17)                      null,
    numeric_value_139           decimal(65, 17)                      null,
    numeric_value_140           decimal(65, 17)                      null,
    numeric_value_141           decimal(65, 17)                      null,
    numeric_value_142           decimal(65, 17)                      null,
    numeric_value_143           decimal(65, 17)                      null,
    numeric_value_144           decimal(65, 17)                      null,
    numeric_value_145           decimal(65, 17)                      null,
    numeric_value_146           decimal(65, 17)                      null,
    numeric_value_147           decimal(65, 17)                      null,
    numeric_value_148           decimal(65, 17)                      null,
    numeric_value_149           decimal(65, 17)                      null,
    numeric_value_150           decimal(65, 17)                      null,
    numeric_value_151           decimal(65, 17)                      null,
    numeric_value_152           decimal(65, 17)                      null,
    numeric_value_153           decimal(65, 17)                      null,
    numeric_value_154           decimal(65, 17)                      null,
    numeric_value_155           decimal(65, 17)                      null,
    unique_numeric_value_2_test decimal(65, 17)                      null,
    unique_text_value_2_test    text                                 null,
    has_avatar                  tinyint(1) default 0                 not null,
    schema_key                  varchar(255)                         not null,
    monolith_id                 bigint                               null,
    hash_key                    varchar(255)                         null,
    created_on                  timestamp  default CURRENT_TIMESTAMP null,
    updated_on                  timestamp  default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    unique_value_ota_1_test     binary(16)                           null,
    unique_value_1_test         text                                 null,
    group_values                json                                 null,
    text_value_99_lower         text as (null),
    text_value_1_lower          text as (null),
    text_value_2_lower          text as (null),
    text_value_3_lower          text as (null),
    text_value_4_lower          text as (null),
    text_value_5_lower          text as (null),
    text_value_6_lower          text as (null),
    text_value_8_lower          text as (null),
    text_value_9_lower          text as (null),
    text_value_10_lower         text as (null),
    text_value_11_lower         text as (null),
    text_value_12_lower         text as (null),
    text_value_13_lower         text as (null),
    text_value_14_lower         text as (null),
    text_value_15_lower         text as (null),
    text_value_16_lower         text as (null),
    text_value_17_lower         text as (null),
    text_value_18_lower         text as (null),
    text_value_19_lower         text as (null),
    text_value_20_lower         text as (null),
    text_value_21_lower         text as (null),
    text_value_22_lower         text as (null),
    text_value_23_lower         text as (null),
    text_value_24_lower         text as (null),
    text_value_25_lower         text as (null),
    text_value_26_lower         text as (null),
    text_value_27_lower         text as (null),
    text_value_28_lower         text as (null),
    text_value_29_lower         text as (null),
    group_values_keys           varchar(500)                         null,
    unique_numeric_value_1      decimal(65, 17)                      null,
    unique_numeric_value_2      decimal(65, 17)                      null,
    unique_text_value_1         text                                 null,
    unique_text_value_2         text                                 null,
    unique_value_1              text                                 null,
    unique_value_ota_1          binary(16)                           null,
    unique_value_ota_2          binary(16)                           null,
    unique_value_2              text                                 null,
    unique_value_ota_3          binary(16)                           null,
    unique_value_3              text                                 null,
    unique_value_ota_4          binary(16)                           null,
    unique_value_4              text                                 null,
    constraint ix_external_id_unique
        unique (workspace_id, external_id(500)),
    constraint ix_sequential_id_unique
        unique (workspace_id, sequential_id),
    constraint unique_constraint_numeric_value_1
        unique (workspace_id, obj_type_id, unique_numeric_value_1),
    constraint unique_constraint_numeric_value_2
        unique (workspace_id, obj_type_id, unique_numeric_value_2),
    constraint unique_constraint_text_value_1
        unique (workspace_id, obj_type_id, unique_text_value_1(500)),
    constraint unique_constraint_text_value_2
        unique (workspace_id, obj_type_id, unique_text_value_2(500)),
    constraint unique_index_value_1
        unique (workspace_id, unique_value_ota_1, unique_value_1(500)),
    constraint unique_index_value_2
        unique (workspace_id, unique_value_ota_2, unique_value_2(500)),
    constraint unique_index_value_3
        unique (workspace_id, unique_value_ota_3, unique_value_3(500)),
    constraint unique_index_value_4
        unique (workspace_id, unique_value_ota_4, unique_value_4(500))
);

-- ALTER TABLE jsm_assets.obj_new
--   ADD FULLTEXT INDEX idx_fts_1(
--     workspace_id,
--     text_value_1,  text_value_2,  text_value_3,  text_value_4,  text_value_5,
--     text_value_6,  text_value_7,  text_value_8,  text_value_9,  text_value_10,
--     text_value_11, text_value_12, text_value_13, text_value_14, text_value_15
--   ) WITH PARSER standard;

create index ix_external_id
    on jsm_assets.obj_new (workspace_id, external_id(500));

create index ix_obj__hash_key
    on jsm_assets.obj_new (hash_key);

create index ix_obj__monolith_id
    on jsm_assets.obj_new (monolith_id);

create index ix_obj_created_on
    on jsm_assets.obj_new (workspace_id, created_on);

create index ix_obj_group_values_keys
    on jsm_assets.obj_new (workspace_id(36), group_values_keys);

create index ix_obj_id
    on jsm_assets.obj_new (workspace_id, id);

create index ix_obj_label
    on jsm_assets.obj_new (workspace_id, label);

create index ix_obj_label_objtypeid
    on jsm_assets.obj_new (workspace_id, label, obj_type_id);

create index ix_obj_numeric_value_1
    on jsm_assets.obj_new (workspace_id, numeric_value_1);

create index ix_obj_numeric_value_2
    on jsm_assets.obj_new (workspace_id, numeric_value_2);

create index ix_obj_numeric_value_3
    on jsm_assets.obj_new (workspace_id, numeric_value_3);

create index ix_obj_numeric_value_4
    on jsm_assets.obj_new (workspace_id, numeric_value_4);

create index ix_obj_numeric_value_5
    on jsm_assets.obj_new (workspace_id, numeric_value_5);

create index ix_obj_numeric_value_6
    on jsm_assets.obj_new (workspace_id, numeric_value_6);

create index ix_obj_numeric_value_7
    on jsm_assets.obj_new (workspace_id, numeric_value_7);

create index ix_obj_numeric_value_8
    on jsm_assets.obj_new (workspace_id, numeric_value_8);

create index ix_obj_numeric_value_9
    on jsm_assets.obj_new (workspace_id, numeric_value_9);

create index ix_obj_text_value_1
    on jsm_assets.obj_new (workspace_id, text_value_1(500));

create index ix_obj_text_value_10
    on jsm_assets.obj_new (workspace_id, text_value_10(500));

create index ix_obj_text_value_11
    on jsm_assets.obj_new (workspace_id, text_value_11(500));

create index ix_obj_text_value_12
    on jsm_assets.obj_new (workspace_id, text_value_12(500));

create index ix_obj_text_value_13
    on jsm_assets.obj_new (workspace_id, text_value_13(500));

create index ix_obj_text_value_14
    on jsm_assets.obj_new (workspace_id, text_value_14(500));

create index ix_obj_text_value_15
    on jsm_assets.obj_new (workspace_id, text_value_15(500));

create index ix_obj_text_value_16
    on jsm_assets.obj_new (workspace_id, text_value_16(500));

create index ix_obj_text_value_17
    on jsm_assets.obj_new (workspace_id, text_value_17(500));

create index ix_obj_text_value_18
    on jsm_assets.obj_new (workspace_id, text_value_18(500));

create index ix_obj_text_value_19
    on jsm_assets.obj_new (workspace_id, text_value_19(500));

create index ix_obj_text_value_2
    on jsm_assets.obj_new (workspace_id, text_value_2(500));

create index ix_obj_text_value_20
    on jsm_assets.obj_new (workspace_id, text_value_20(500));

create index ix_obj_text_value_21
    on jsm_assets.obj_new (workspace_id, text_value_21(500));

create index ix_obj_text_value_22
    on jsm_assets.obj_new (workspace_id, text_value_22(500));

create index ix_obj_text_value_23
    on jsm_assets.obj_new (workspace_id, text_value_23(500));

create index ix_obj_text_value_24
    on jsm_assets.obj_new (workspace_id, text_value_24(500));

create index ix_obj_text_value_25
    on jsm_assets.obj_new (workspace_id, text_value_25(500));

create index ix_obj_text_value_26
    on jsm_assets.obj_new (workspace_id, text_value_26(500));

create index ix_obj_text_value_27
    on jsm_assets.obj_new (workspace_id, text_value_27(500));

create index ix_obj_text_value_28
    on jsm_assets.obj_new (workspace_id, text_value_28(500));

create index ix_obj_text_value_29
    on jsm_assets.obj_new (workspace_id, text_value_29(500));

create index ix_obj_text_value_3
    on jsm_assets.obj_new (workspace_id, text_value_3(500));

create index ix_obj_text_value_4
    on jsm_assets.obj_new (workspace_id, text_value_4(500));

create index ix_obj_text_value_5
    on jsm_assets.obj_new (workspace_id, text_value_5(500));

create index ix_obj_text_value_6
    on jsm_assets.obj_new (workspace_id, text_value_6(500));

create index ix_obj_text_value_7
    on jsm_assets.obj_new (workspace_id, text_value_7(500));

create index ix_obj_text_value_8
    on jsm_assets.obj_new (workspace_id, text_value_8(500));

create index ix_obj_text_value_9
    on jsm_assets.obj_new (workspace_id, text_value_9(500));

create index ix_obj_type_id
    on jsm_assets.obj_new (workspace_id, obj_type_id);

create index ix_obj_unique_numeric_value_1
    on jsm_assets.obj_new (workspace_id, unique_numeric_value_1);

create index ix_obj_unique_numeric_value_2
    on jsm_assets.obj_new (workspace_id, unique_numeric_value_2);

create index ix_obj_unique_text_value_1
    on jsm_assets.obj_new (workspace_id, unique_text_value_1(500));

create index ix_obj_unique_text_value_2
    on jsm_assets.obj_new (workspace_id, unique_text_value_2(500));

create index ix_obj_updated_on
    on jsm_assets.obj_new (workspace_id, updated_on);

create index ix_schema_id
    on jsm_assets.obj_new (workspace_id, schema_id);



create table jsm_assets.obj_relationship_new
(
    id                        binary(16)   not null
        primary key,
    workspace_id              varchar(255) not null,
    object_id                 binary(16)   not null,
    referenced_object_id      binary(16)   not null,
    object_type_attribute_id  binary(16)   not null,
    object_type_id            binary(16)   not null,
    referenced_object_type_id binary(16)   not null
);

create index idx_obj_rel_workspace_refobj_attrid
    on jsm_assets.obj_relationship_new (workspace_id(36), referenced_object_id, object_type_attribute_id);

create index ix_obj_rel_object_id
    on jsm_assets.obj_relationship_new (object_id);

create index ix_obj_rel_referenced_object_id
    on jsm_assets.obj_relationship_new (referenced_object_id);

create index ix_obj_relationship_obj_id_attribute_id
    on jsm_assets.obj_relationship_new (workspace_id, object_id, object_type_attribute_id);

create index ix_obj_relationship_obj_type_attr_id
    on jsm_assets.obj_relationship_new (workspace_id, object_type_attribute_id);

create index ix_obj_relationship_obj_type_id
    on jsm_assets.obj_relationship_new (workspace_id, object_type_id);

create index ix_obj_relationship_unique
    on jsm_assets.obj_relationship_new (workspace_id, object_id, referenced_object_id, object_type_attribute_id);

