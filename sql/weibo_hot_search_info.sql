CREATE EXTERNAL TABLE `yuqing.ods_weibo_hotsearch_detail_inc`(
	  `_id` string COMMENT 'id', 
	  `content` string COMMENT '文本全文',
	  `url` string COMMENT 'url',
	  `mid` string COMMENT '站内消息ID',
	  `source` string COMMENT '发布超话来源', 
	  `time` string COMMENT '发布时间',
	  `publish_time` bigint COMMENT '发布时间 时间戳',
	  `picture_urls` string COMMENT '图片url（通过，连接）', 
	  `video_urls` string COMMENT '视频url（通过，连接）',
	  `repost_count` bigint COMMENT '转发数',
	  `comment_count` bigint COMMENT '评论数',
	  `like_count` bigint COMMENT '点赞数',
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
	  `content_type` int COMMENT '内容类型',
	  `message_type` int COMMENT '数据类型',
	  `site_name` string COMMENT '网站名称', 
	  `site_id` string COMMENT '网站id',
	  `channel` int COMMENT '数据所属渠道',
	  `id` string COMMENT 'id',
	  `root_mid` string COMMENT 'root_mid',
	  `root_author_id` string COMMENT 'root_author_id',
	  `root_publish_time` bigint COMMENT '转发原帖发布时间'
	  
	  )
	COMMENT 'ods层微博热搜帖子表'
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
	  'hdfs://agent0:8020/warehouse/yuqing/ods/ods_weibo_hotsearch_detail_inc'
	