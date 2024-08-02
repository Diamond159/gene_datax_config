drop table if exists dwd_hotsearch_comment_inc;
create external table dwd_hotsearch_comment_inc(
      `_id` string COMMENT 'id', 
	  `brief_text` string COMMENT '简短文本',
	  `author_name` string COMMENT '评论作者名',
	  `attention_point` bigint COMMENT '关注度',
	  `content` string COMMENT '内容',
	  `themes` string COMMENT '主题词',
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
	  `site_name` string COMMENT '网站名称', 
	  `site_id` string COMMENT '网站id',
	  `channel` int COMMENT '数据所属渠道',
	  `id` string COMMENT 'id',
	  
	  `insert_time` bigint COMMENT '数据推送接受时间戳',
	  `sentiment` string COMMENT '情感倾向',
	  `categories` string COMMENT '所属领域',
	  `sensitive` string COMMENT '敏感性',
	  `author_region` string COMMENT '所属区域',
	  `verified` boolean COMMENT '是否认证'
		
		
) comment '热搜帖子评论信息表'
PARTITIONED BY (`operate_date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/warehouse/yuqing/dwd/dwd_hotsearch_comment_inc/'
TBLPROPERTIES ('orc.compress' = 'snappy');



