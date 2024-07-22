CREATE EXTERNAL TABLE `yuqing.ods_weibo_hotsearch_download_inc`(
	  `_id` string COMMENT 'id', 
	  `source_url` string COMMENT '来源地址',
	  `local_path` string COMMENT '本地地址',
	  `remote_path` string COMMENT '远程地址',
	  `save_time` string COMMENT '保存时间',
	  `time_stamp` bigint COMMENT '保存时间戳'
	  
	  
	  )
	COMMENT 'ods层微博热搜帖子信息下载表'
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
	  'hdfs://agent0:8020/warehouse/yuqing/ods/ods_weibo_hotsearch_download_inc'