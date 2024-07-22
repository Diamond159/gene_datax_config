CREATE EXTERNAL TABLE `yuqing.ods_weibo_hotsearch_comment_inc`(
 	  `_id` string COMMENT 'id', 
	  `author_name` string COMMENT '评论作者名',
	  `like_count` bigint COMMENT '点赞数',
	  `repost_count` bigint COMMENT '转发数',
	  `content` string COMMENT '内容',
	  `author_id` string COMMENT '用户id',
      `publish_time` bigint COMMENT '发布时间',
	  `post_location` string COMMENT '发布地址',
	  `source_url` string COMMENT '评论来源帖子',
	  `save_time` string COMMENT '采集时间',
	  `gather_time` bigint COMMENT '采集时间戳',
	  `data_provider` string COMMENT '数据供应商',
	  `url` string COMMENT 'url',
	  `root_mid` string COMMENT 'root_mid',
	  `root_author_id` string COMMENT 'root_author_id',
	  `root_author_name` string COMMENT 'root_author_name',
	  `mid` string COMMENT '站内消息ID',
	  `content_type` int COMMENT '内容类型',
	  `message_type` int COMMENT '数据类型',
	  `site_name` string COMMENT '网站名称', 
	  `site_id` string COMMENT '网站id',
	  `channel` int COMMENT '数据所属渠道',
	  `id` string COMMENT 'id'
	  )
	COMMENT 'ods层微博热搜评论表'
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
	  'hdfs://agent0:8020/warehouse/yuqing/ods/ods_weibo_hotsearch_comment_inc'