CREATE EXTERNAL TABLE `yuqing.ods_weibo_hotsearch_userinfo_inc`(
	  `_id` string COMMENT 'id', 
	  `name` string COMMENT '用户姓名',
	  `followers_count` bigint COMMENT '粉丝数',
	  `friends_count` bigint COMMENT '朋友数',
	  `verified_info` string COMMENT '认证身份',
	  `verified` boolean COMMENT '是否认证',
	  `profile_image_url` string COMMENT '背景图片',
	  `register_time` bigint COMMENT '注册时间（时间戳）',
	  `ip_home` string COMMENT 'ip属地',
	  `uid` bigint COMMENT '用户唯一标识符',
	  `statues_count` bigint COMMENT '用户所有帖子数',
	  `description` string COMMENT '简介',
	  `birthday` string COMMENT '生日',
	  `gender` string COMMENT '性别',
	  `source_url` string COMMENT '用户主页地址',
	  `save_time` string COMMENT '采集时间',
	  `gather_time` bigint COMMENT '采集时间戳',
	  `data_provider` string COMMENT '数据提供商',
	  `site_name` string COMMENT '网站名称', 
	  `site_id` string COMMENT '网站id',
	  `channel` int COMMENT '数据所属渠道'
	  
	  
	  )
	COMMENT 'ods层微博热搜帖子用户详细信息表'
	PARTITIONED BY ( 
	  `operate_date` string)
	ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
	WITH SERDEPROPERTIES ( 
	  'field.delim'='\t', 
	  'serialization.format'='\t', 
	  'serialization.null.format'='') 
	STORED AS INPUTFORMAT 
	  'org.apache.hadoop.mapred.TextInputFormat' 
	OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	LOCATION
	  'hdfs://agent0:8020/warehouse/yuqing/ods/ods_weibo_hotsearch_userinfo_inc'
	
	
	