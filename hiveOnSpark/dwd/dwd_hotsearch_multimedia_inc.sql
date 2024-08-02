drop table if exists dwd_hotsearch_multimedia_inc;
create external table dwd_hotsearch_multimedia_inc(
      `_id` string COMMENT 'id', 
	  `brief_text` string COMMENT '文本简介',
	  `content` string COMMENT '文本全文',
	  `url` string COMMENT 'url',
	  `mid` string COMMENT '站内消息ID',
	  `source` string COMMENT '发布超话来源', 
	  `time` string COMMENT '发布时间',
	  `publish_time` bigint COMMENT '发布时间 时间戳',
	  `hot_point` bigint COMMENT '热点数',
	  `author_name` string COMMENT '用户名',
	  `author_id` string COMMENT '用户id',
	  `root_url` string COMMENT '转发原帖url',
	  `root_time` string COMMENT '原帖发布时间',
	  `root_author_name` string COMMENT '原帖作者名',
	  `root_content` string COMMENT '原帖内容',
	  `user_url` string COMMENT '用户主页url',
	  `source_url` string COMMENT '帖子来源搜索列表',
	  `save_time` string COMMENT '采集时间',
	  `gather_time` bigint COMMENT '采集时间戳', 
	  `data_provider` string COMMENT '数据供应商',
	  `themes` string COMMENT '主题词',
	  `site_name` string COMMENT '网站名称', 
	  `site_id` string COMMENT '网站id',
	  `channel` int COMMENT '数据所属渠道',
	  `id` string COMMENT 'id',
	  `root_mid` string COMMENT 'root_mid',
	  `root_author_id` string COMMENT 'root_author_id',
	  `root_publish_time` bigint COMMENT '转发原帖发布时间',
	  
	  `insert_time` bigint COMMENT '数据推送接受时间戳',
	  `sentiment` string COMMENT '情感倾向',
	  `categories` string COMMENT '所属领域',
	  `sensitive` string COMMENT '敏感性',
	  `author_region` string COMMENT '所属区域',
	  `verified` boolean COMMENT '是否认证'
	  
		
		
) comment '热搜帖子视频信息表'
PARTITIONED BY (`operate_date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/warehouse/yuqing/dwd/dwd_hotsearch_multimedia_inc/'
TBLPROPERTIES ('orc.compress' = 'snappy');



