with
    twitter_data as (
        select
            cast(date as date) as date,
            cast(null as int64) as add_to_cart,
            cast(clicks as int64) as clicks,
            cast(comments as int64) as comments,
            cast(engagements as int64) as engagements,
            cast(impressions as int64) as impressions,
            cast(null as int64) as installs,
            cast(likes as int64) as likes,
            cast(url_clicks as int64) as link_clicks,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            cast(null as int64) as purchase,
            cast(null as int64) as registrations,
            cast(null as int64) as revenue,
            cast(retweets as int64) as shares,
            cast(spend as int64) as spend,
            cast(null as int64) as total_conversions,
            cast(video_total_views as int64) as video_views,
            cast(null as string) as ad_id,
            cast(null as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            cast(channel as string) as channel,
            cast(null as string) as creative_id,
            cast(null as string) as placement_id
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
    ),
    tiktok_data as (
        select
            cast(date as date) as date,
            cast(add_to_cart as int64) as add_to_cart,
            cast(clicks as int64) as clicks,
            cast(null as int64) as comments,
            cast(null as int64) as engagements,
            cast(impressions as int64) as impressions,
            cast(skan_app_install as int64) as installs,
            cast(null as int64) as likes,
            cast(null as int64) as link_clicks,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            cast(purchase as int64) as purchase,
            cast(registrations as int64) as registrations,
            cast(null as int64) as revenue,
            cast(null as int64) as shares,
            cast(spend as int64) as spend,
            cast(conversions as int64) as total_conversions,
            cast(video_views as int64) as video_views,
            cast(ad_id as string) as ad_id,
            cast(adgroup_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            cast(channel as string) as channel,
            cast(null as string) as creative_id,
            cast(null as string) as placement_id
        from {{ ref("src_ads_tiktok_ads_all_data") }}
    ),
    bing_data as (
        select
            cast(date as date) as date,
            cast(null as int64) as add_to_cart,
            cast(clicks as int64) as clicks,
            cast(null as int64) as comments,
            cast(null as int64) as engagements,
            cast(imps as int64) as impressions,
            cast(null as int64) as installs,
            cast(null as int64) as likes,
            cast(null as int64) as link_clicks,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            cast(null as int64) as purchase,
            cast(null as int64) as registrations,
            cast(revenue as int64) as revenue,
            cast(null as int64) as shares,
            cast(spend as int64) as spend,
            cast(conv as int64) as total_conversions,
            cast(null as int64) as video_views,
            cast(ad_id as string) as ad_id,
            cast(adset_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            cast(channel as string) as channel,
            cast(null as string) as creative_id,
            cast(null as string) as placement_id
        from {{ ref("src_ads_bing_all_data") }}
    ),
    facebook_data as (
        select
            cast(date as date) as date,
            cast(add_to_cart as int64) as add_to_cart,
            cast(clicks as int64) as clicks,
            cast(comments as int64) as comments,
            cast(
                views + likes + comments + shares + inline_link_clicks as int64
            ) as engagements,
            cast(impressions as int64) as impressions,
            cast(mobile_app_install as int64) as installs,
            cast(likes as int64) as likes,
            cast(inline_link_clicks as int64) as link_clicks,
            cast(clicks / impressions as int64) as post_click_conversions,
            cast(views / impressions as int64) as post_view_conversions,
            cast(null as int64) as posts,
            cast(purchase as int64) as purchase,
            cast(complete_registration as int64) as registrations,
            cast(purchase_value as int64) as revenue,
            cast(shares as int64) as shares,
            cast(spend as int64) as spend,
            cast(purchase as int64) as total_conversions,
            cast(null as int64) as video_views,
            cast(ad_id as string) as ad_id,
            cast(adset_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            cast(channel as string) as channel,
            cast(creative_id as string) as creative_id,
            cast(null as string) as placement_id
        from {{ ref("src_ads_creative_facebook_all_data") }}
    )

-- combine all data
select *
from twitter_data
union all
select *
from tiktok_data
union all
select *
from bing_data
union all
select *
from facebook_data
