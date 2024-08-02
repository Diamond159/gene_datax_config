
	--图文表dwd层
SELECT
    a.`_id` as `_id`,
    substr(a.content,1, 50) as `brief_text`,
    a.content as content,
    a.url as url,
    a.mid as mid,
    a.source as source,
    a.`time` as `time`,
    a.publish_time as publish_time,
    split(regexp_replace(regexp_replace(a.picture_urls, '[\\[\\]" ]', ''), ',', ' '), ' ') as picture_urls,
    round(a.like_count*0.5+a.comment_count*0.9+a.repost_count*0.8) as hot_point,
    a.author_name as author_name,
    a.author_id as author_id,
    a.root_url as root_url,
    a.root_time as root_time,
    a.root_author_name as root_author_name,
    a.root_content as root_content,
    a.user_url as user_url,
    a.source_url as source_url,
    a.save_time as save_time,
    a.gather_time as gather_time,
    a.data_provider as data_provider,
    a.themes as themes,
    a.site_name as site_name,
    a.site_id as site_id,
    a.channel as channel,
    a.id as id,
    a.root_mid as root_mid,
    a.root_author_id as root_author_id,
    a.root_publish_time as root_publish_time,
    '$CURRENT_TIME_STAMP' as insert_time,
    collect_list(b.local_path) AS local_paths
FROM
    (
        SELECT *
        FROM yuqing.ods_weibo_hotsearch_detail_inc
        WHERE 
        `_id` RLIKE '[0-9a-fA-F]{24}' 
        and length(content) > 0
		and content_type IN (1, 2)
        and message_type IN (1, 3)
		
		-- and operate_date = $
    ) AS a
JOIN
    yuqing.ods_weibo_hotsearch_download_inc AS b
    WHERE concat('[', concat_ws(',', a.picture_urls), ']') RLIKE b.source_url
GROUP BY
    a.`_id`,a.content,a.url,a.mid,a.source,a.`time`,a.publish_time,a.like_count,a.comment_count,a.repost_count,a.author_name,a.author_id,
    a.root_url,a.root_time,a.root_author_name,a.root_content,a.user_url,a.source_url,a.save_time,a.gather_time,a.data_provider,
    a.themes,a.site_name,a.site_id,a.channel,a.id,a.root_mid,a.root_author_id,a.root_publish_time,a.picture_urls;
	
	
	
	
	
	--dwd视频表
	
SELECT
    a.`_id` as `_id`,
    substr(a.content,1, 50) as `brief_text`,
    a.content as content,
    a.url as url,
    a.mid as mid,
    a.source as source,
    a.`time` as `time`,
    a.publish_time as publish_time,
    split(regexp_replace(regexp_replace(a.video_urls, '[\\[\\]" ]', ''), ',', ' '), ' ') as video_urls,
    round(a.like_count*0.5+a.comment_count*0.9+a.repost_count*0.8) as hot_point,
    a.author_name as author_name,
    a.author_id as author_id,
    a.root_url as root_url,
    a.root_time as root_time,
    a.root_author_name as root_author_name,
    a.root_content as root_content,
    a.user_url as user_url,
    a.source_url as source_url,
    a.save_time as save_time,
    a.gather_time as gather_time,
    a.data_provider as data_provider,
    a.themes as themes,
    a.site_name as site_name,
    a.site_id as site_id,
    a.channel as channel,
    a.id as id,
    a.root_mid as root_mid,
    a.root_author_id as root_author_id,
    a.root_publish_time as root_publish_time,
    '$CURRENT_TIME_STAMP' as insert_time,
    collect_list(b.local_path) AS local_paths
FROM
    (
        SELECT *
        FROM yuqing.ods_weibo_hotsearch_detail_inc
        WHERE 
        `_id` RLIKE '[0-9a-fA-F]{24}' 
        and length(content) > 0
		and content_type = 3
        AND message_type IN (1, 3)
    ) AS a
JOIN
    yuqing.ods_weibo_hotsearch_download_inc AS b
    WHERE concat('[', concat_ws(',', a.video_urls), ']') RLIKE b.source_url
GROUP BY
    a.`_id`,a.content,a.url,a.mid,a.source,a.`time`,a.publish_time,a.like_count,a.comment_count,a.repost_count,a.author_name,a.author_id,
    a.root_url,a.root_time,a.root_author_name,a.root_content,a.user_url,a.source_url,a.save_time,a.gather_time,a.data_provider,
    a.themes,a.site_name,a.site_id,a.channel,a.id,a.root_mid,a.root_author_id,a.root_publish_time,a.video_urls;
	
	
	
	
	
	
	
    --用户表dwd	
	
select `_id`,
        name,
        round(followers_count*0.5+friends_count*10+statues_count*0.1) as impact_index,
        verified_info,
        verified,
        profile_image_url,
        register_time,
        ip_home,
        uid,
        description,
        birthday,
        gender,
        source_url,
        save_time,
        gather_time,
        data_provider,
        site_name,
        site_id,
        channel,
        '{0}' as insert_time
        
from yuqing.ods_weibo_hotsearch_userinfo_inc
`_id` REGEXP '^[0-9a-fA-F]+$' AND length(`_id`) = 24
and length(name) > 0











	--dwd_hotsearch_multimedia_inc表

SELECT
    `_id`,
    substr(content,1, 50) as `brief_text`,
    content,
    url,
    mid,
    source,
    `time`,
    publish_time,
    like_count*0.5+comment_count*0.9+repost_count*0.8 as hot_point,
    author_name,
    author_id,
    root_url,
    root_time,
    root_author_name,
    root_content,
    user_url,
    source_url,
    save_time,
    gather_time,
    data_provider,
    themes,
    site_name,
    site_id,
    channel,
    id,
    root_mid,
    root_author_id,
    root_publish_time
    
    FROM yuqing.ods_weibo_hotsearch_detail_inc
    where `_id` RLIKE '[0-9a-fA-F]{24}' and length(content) > 0
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	--   后续应该可用于twitter
SELECT `_id`,
        substr(content,1, 50) as `brief_text`,
        author_name,
        like_count*0.5+repost_count*0.8 as attention_point,
        content,
        themes,
        author_id,
        publish_time,
        '' as post_location,
        source_url,
        save_time,
        gather_time,
        data_provider,
        url,
        root_mid,
        root_author_id,
        root_author_name,
        mid,
        site_name,
        site_id,
        channel,
        id,
        '$CURRENT_TIME_STAMP' as insert_time

from yuqing.ods_weibo_hotsearch_detail_inc
where  message_type=2 and `_id` RLIKE '[0-9a-fA-F]{24}' and length(content) > 0

union 

select a.`_id` as `_id`,
        substr(b.content,1, 50) as `brief_text`,
        a.author_name as author_name,
        a.like_count*0.5+a.repost_count*0.8 as attention_point,
        a.content as content,
        b.themes as themes,
        a.author_id as author_id,
        a.publish_time as publish_time,
        a.post_location as post_location,
        a.source_url as source_url,
        a.save_time as save_time,
        a.gather_time as gather_time,
        a.data_provider as data_provider,
        a.url as url,
        a.root_mid as root_mid,
        a.root_author_id as root_author_id,
        a.root_author_name as root_author_name,
        a.mid as mid,
        a.site_name as site_name,
        a.site_id as site_id,
        a.channel as channel,
        a.id as id,
        '$CURRENT_TIME_STAMP' as insert_time
        
from (select * from yuqing.ods_weibo_hotsearch_comment_inc 
        where `_id` RLIKE '[0-9a-fA-F]{24}' and length(content) > 0
        ) as a
join yuqing.ods_weibo_hotsearch_detail_inc as b
on a.root_mid=b.mid






	--微博评论dwd层
select a.`_id` as `_id`,
        b.content as `brief_text`,
        a.author_name as author_name,
        a.like_count*0.5+a.repost_count*0.8 as attention_point,
        a.content as content,
        b.themes as themes,
        a.author_id as author_id,
        a.publish_time as publish_time,
        a.post_location as post_location,
        a.source_url as source_url,
        a.save_time as save_time,
        a.gather_time as gather_time,
        a.data_provider as data_provider,
        a.url as url,
        a.root_mid as root_mid,
        a.root_author_id as root_author_id,
        a.root_author_name as root_author_name,
        a.mid as mid,
        a.site_name as site_name,
        a.site_id as site_id,
        a.channel as channel,
        a.id as id,
        '$CURRENT_TIME_STAMP' as insert_time
        
from (select * from yuqing.ods_weibo_hotsearch_comment_inc 
        where `_id` REGEXP '^[0-9a-fA-F]+$' AND length(`_id`) = 24
		and length(content) > 0
        ) as a
join (select distinct content,themes,mid from yuqing.ods_weibo_hotsearch_detail_inc) as b
on a.root_mid=b.mid









create external table test(
      `_id` string COMMENT 'id', 
	  `content` string COMMENT '文本全文'
) comment '	测试信息表'
PARTITIONED BY (`operate_date` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/warehouse/yuqing/dws/test/'
TBLPROPERTIES ('orc.compress' = 'snappy');

	
	
drop table if exists test;



--图片以及视频前10具体信息， 包括热度变化     **未添加 地区

WITH base_query AS (
    SELECT bq.`_id`, bq.content, bq.hot_point, bq.impact_index, bq.themes, bq.url, bq.source, bq.`time`, bq.aurls, bq.author_name, bq.author_id,
           bq.verified_info, bq.author_verified, bq.gender, bq.data_provider, bq.site_name, bq.local_path, bq.sentiment, bq.categories, bq.sensitive, bq.author_region, bq.verified,
           row_number() OVER (ORDER BY bq.hot_point DESC, bq.impact_index DESC) AS `rank`
    FROM (
        SELECT `_id`, content, recent_days, hot_point, 0 as impact_index, themes, url, source, `time`, video_urls AS aurls, author_name, author_id,
               verified_info, author_verified, gender, data_provider, site_name, local_path, sentiment, categories, sensitive, author_region, verified
        FROM yuqing.dws_hotsearch_video_nd_inc
        WHERE `recent_days` = 1
        UNION ALL
        SELECT `_id`, content, recent_days, hot_point, 0 as impact_index, themes, url, source, `time`, picture_urls AS aurls, author_name, author_id,
               verified_info, author_verified, gender, data_provider, site_name, local_path, sentiment, categories, sensitive, author_region, verified
        FROM yuqing.dws_hotsearch_textimage_nd_inc
        WHERE `recent_days` = 1
    ) bq
)
SELECT bq.`_id`, bq.content, bq.hot_point, bq.impact_index, bq.themes, bq.url, bq.source, bq.`time`, bq.aurls, bq.author_name, bq.author_id,
       bq.verified_info, bq.author_verified, bq.gender, bq.data_provider, bq.site_name, bq.local_path, bq.sentiment, bq.categories, bq.sensitive, bq.author_region, bq.verified,
       CASE WHEN bq.hot_point IS NOT NULL AND COALESCE(bqr.hot_point, 0) <> 0 THEN bq.hot_point - bqr.hot_point ELSE 0 END AS hot_point_diff
FROM base_query bq
LEFT JOIN (
    SELECT `_id`, hot_point
    FROM (
        SELECT `_id`, hot_point, row_number() OVER (ORDER BY hot_point DESC) AS `rank`
        FROM (
            SELECT `_id`, hot_point
            FROM yuqing.dws_hotsearch_video_nd_inc
            WHERE `recent_days` = 3
            UNION ALL
            SELECT `_id`, hot_point
            FROM yuqing.dws_hotsearch_textimage_nd_inc
            WHERE `recent_days` = 3
        ) tmp
    ) bqr
    WHERE `rank` <= 10
) bqr ON bq.`_id` = bqr.`_id`
WHERE bq.`rank` <= 10;




--  图片以及文章一起其他媒体前10具体信息  分成三个区

SELECT `_id`, content, hot_point, impact_index, themes, url, source, `time`, picture_urls, author_name, author_id,
       verified_info, author_verified, gender, data_provider, site_name, local_path, sentiment, categories, sensitive, author_region, verified,recent_days
FROM (
    SELECT `_id`, content, hot_point, impact_index, themes, url, source, `time`, picture_urls, author_name, author_id,
           verified_info, author_verified, gender, data_provider, site_name, local_path, sentiment, categories, sensitive, author_region, verified,recent_days,
           row_number() OVER (PARTITION BY recent_days ORDER BY hot_point DESC, impact_index DESC) AS `rank` 
    FROM (
        
        SELECT `_id`, content, recent_days, hot_point, impact_index, themes, url, source, `time`, picture_urls, author_name, author_id,
               verified_info, author_verified, gender, data_provider, site_name, local_path, sentiment, categories, sensitive, author_region, verified
        FROM yuqing.dws_hotsearch_textimage_nd_inc
    ) subquery
) tmp
WHERE `rank` <= 10;




--  全平台视频前10信息   分区

SELECT `_id`, content, hot_point, impact_index, themes, url, source, `time`, video_urls, author_name, author_id,
       verified_info, author_verified, gender, data_provider, site_name, local_path, sentiment, categories, sensitive, author_region, verified, recent_days
FROM (
    SELECT `_id`, content, hot_point, impact_index, themes, url, source, `time`, video_urls, author_name, author_id,
           verified_info, author_verified, gender, data_provider, site_name, local_path, sentiment, categories, sensitive, author_region, verified,recent_days,
           row_number() OVER (PARTITION BY recent_days ORDER BY hot_point DESC, impact_index DESC) AS `rank` 
    FROM (
        SELECT `_id`, content, recent_days, hot_point, impact_index, themes, url, source, `time`, video_urls, author_name, author_id,
               verified_info, author_verified, gender, data_provider, site_name, local_path, sentiment, categories, sensitive, author_region, verified
        FROM yuqing.dws_hotsearch_video_nd_inc
        --UNION ALL
        --待添加其他平台
    ) subquery
) tmp
WHERE `rank` <= 10;










--微博评论信息

--  每天更新分区 day100    通过  overwrite   参数通过 py 算 

select `_id`, brief_text, author_name, attention_point, content, themes, author_id, impact_index, verified_info, verified, gender, publish_time, post_location, source_url, save_time, gather_time, data_provider, url, mid, site_name, site_id, channel, id, insert_time, sentiment, categories, sensitive, author_region, comverified, recent_days
from
(select `_id`, brief_text, author_name, attention_point, content, themes, author_id, impact_index, verified_info, verified, gender, publish_time, post_location, source_url, save_time, gather_time, data_provider, url, mid, site_name, site_id, channel, id, insert_time, sentiment, categories, sensitive, author_region, comverified, recent_days 
from yuqing.dws_hotsearch_comment_nd_inc
where recent_days=100
union
select `_id`, brief_text, author_name, attention_point, content, themes, author_id, impact_index, verified_info, verified, gender, publish_time, post_location, source_url, save_time, gather_time, data_provider, url, mid, site_name, site_id, channel, id, insert_time, sentiment, categories, sensitive, author_region, comverified, 100 as recent_days 
from yuqing.dws_hotsearch_comment_nd_inc
where recent_days=7 and gather_time > '{小于7}') as subquery;



--   每天更新分区 day7    通过overwrite   参数通过  

select `_id`, brief_text, author_name, attention_point, content, themes, author_id, impact_index, verified_info, verified, gender, publish_time, post_location, source_url, save_time, gather_time, data_provider, url, mid, site_name, site_id, channel, id, insert_time, sentiment, categories, sensitive, author_region, comverified, 7 as recent_days from
(select `_id`, brief_text, author_name, attention_point, content, themes, author_id, impact_index, verified_info, verified, gender, publish_time, post_location, source_url, save_time, gather_time, data_provider, url, mid, site_name, site_id, channel, id, insert_time, sentiment, categories, sensitive, author_region, comverified,7 as recent_days 
from yuqing.dws_hotsearch_comment_nd_inc
where recent_days=7 and gather_time < '{小于7}'
union 
select `_id`, brief_text, author_name, attention_point, content, themes, author_id, impact_index, verified_info, verified, gender, publish_time, post_location, source_url, save_time, gather_time, data_provider, url, mid, site_name, site_id, channel, id, insert_time, sentiment, categories, sensitive, author_region, comverified, recent_days 
from yuqing.dws_hotsearch_comment_nd_inc
where recent_days=3 and gather_time > '{大于3}' 
) AS subquery;



--   每天更新分区 day3    通过overwrite   参数通过  

select `_id`, brief_text, author_name, attention_point, content, themes, author_id, impact_index, verified_info, verified, gender, publish_time, post_location, source_url, save_time, gather_time, data_provider, url, mid, site_name, site_id, channel, id, insert_time, sentiment, categories, sensitive, author_region, comverified, recent_days 
from yuqing.dws_hotsearch_comment_nd_inc
where recent_days=2 



--   每天更新分区 day2    通过overwrite   参数通过  

select `_id`, brief_text, author_name, attention_point, content, themes, author_id, impact_index, verified_info, verified, gender, publish_time, post_location, source_url, save_time, gather_time, data_provider, url, mid, site_name, site_id, channel, id, insert_time, sentiment, categories, sensitive, author_region, comverified, recent_days 
from yuqing.dws_hotsearch_comment_nd_inc
where recent_days=1;




------------------------------------------------------------------------
--微博图片信息

--   每天更新分区 day100    通过into   参数通过 

SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, picture_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 100 as recent_days
FROM (SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, picture_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 100 as recent_days
FROM yuqing.dws_hotsearch_textimage_nd_inc WHERE recent_days = 7 AND gather_time > '{小于7}'




--   每天更新分区 day7    通过overwrite   参数通过 


SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, picture_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 7 as recent_days
FROM (SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, picture_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 7 as recent_days
FROM yuqing.dws_hotsearch_textimage_nd_inc WHERE recent_days = 7 AND gather_time < '{小于7}'
UNION
SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, picture_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, recent_days
FROM yuqing.dws_hotsearch_textimage_nd_inc WHERE recent_days = 3) AS subquery;


--   每天更新分区 day3    通过overwrite   参数通过 
insert overwrite table aa
SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, picture_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 3 as recent_days
FROM yuqing.dws_hotsearch_textimage_nd_inc WHERE recent_days = 2


--   每天更新分区 day2    通过overwrite   参数通过 

SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, picture_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 2 as recent_days
FROM yuqing.dws_hotsearch_textimage_nd_inc WHERE recent_days = 1	



------------------------------------------------------------------------------
--微博视频信息

--      每天更新分区 day100    通过into   参数通过 
SELECT  `_id`, brief_text, content, url, mid, source, `time`, publish_time, video_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 100 as recent_days 
FROM yuqing.dws_hotsearch_video_nd_inc WHERE recent_days= 7 and gather_time > '{}'

--      每天更新分区 day7    通过overwrite   参数通过 

SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, video_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, recent_days 
FROM (SELECT  `_id`, brief_text, content, url, mid, source, `time`, publish_time, video_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 7 as recent_days 
FROM yuqing.dws_hotsearch_video_nd_inc WHERE recent_days= 3 and gather_time > '{}'
UNION
SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, video_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, recent_days
FROM yuqing.dws_hotsearch_video_nd_inc WHERE recent_days= 7 AND gather_time < '{}'


--      每天更新分区 day3    通过overwrite   参数通过 

SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, video_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 3 AS recent_days
FROM yuqing.dws_hotsearch_video_nd_inc WHERE recent_days= 2

--      每天更新分区 day2    通过overwrite   参数通过 

SELECT `_id`, brief_text, content, url, mid, source, `time`, publish_time, video_urls, hot_point, author_name, author_id, impact_index, verified_info, author_verified, gender, user_url, source_url, save_time, gather_time, data_provider, themes, site_name, site_id, channel, id, insert_time, local_path, sentiment, categories, sensitive, author_region, verified, 2 AS recent_days
FROM yuqing.dws_hotsearch_video_nd_inc WHERE recent_days= 1