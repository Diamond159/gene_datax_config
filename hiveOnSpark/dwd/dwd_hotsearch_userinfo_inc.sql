drop table if exists dwd_hotsearch_userinfo_inc;
create external table dwd_hotsearch_userinfo_inc(
      `_id` string COMMENT 'id', 
	  `name` string COMMENT '用户姓名',
	  `impact_index` bigint COMMENT '影响指数',
	  `verified_info` string COMMENT '认证身份',
	  `verified` boolean COMMENT '是否认证',
	  `profile_image_url` string COMMENT '背景图片',
	  `register_time` bigint COMMENT '注册时间（时间戳）',
	  `ip_home` string COMMENT 'ip属地',
	  `uid` bigint COMMENT '用户唯一标识符',
	  `description` string COMMENT '简介',
	  `birthday` string COMMENT '生日',
	  `gender` string COMMENT '性别',
	  `source_url` string COMMENT '用户主页地址',
	  `save_time` string COMMENT '采集时间',
	  `gather_time` bigint COMMENT '采集时间戳',
	  `data_provider` string COMMENT '数据提供商',
	  `site_name` string COMMENT '网站名称', 
	  `site_id` string COMMENT '网站id',
	  `channel` int COMMENT '数据所属渠道',
	  `author_region` string COMMENT '所属区域',
	  
	  `insert_time` bigint COMMENT '数据推送接受时间戳'
	  
	  
		
		
) comment '热搜帖子相关用户信息表'
PARTITIONED BY (`operate_date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/warehouse/yuqing/dwd/dwd_hotsearch_userinfo_inc/'
TBLPROPERTIES ('orc.compress' = 'snappy');



