{{ config (
    alias = target.database + '_tiktok_ad_performance'
)}}

SELECT 
CASE WHEN adgroup_name ~* ('Plymouth Meeting') THEN 'Franchise'
    WHEN adgroup_name ~* ('NYC|LA|Rittenhouse') THEN 'Heyday Owned'
END as store_type,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE WHEN adgroup_name ~* 'NYC' THEN 'NYC'
    WHEN adgroup_name ~* 'LA' THEN 'LA'
    WHEN adgroup_name ~* 'Rittenhouse' THEN 'Rittenhouse'
    WHEN adgroup_name ~* 'Plymouth Meeting' THEN 'Plymouth Meeting'
END as market,
adgroup_name,
adgroup_id,
adgroup_status,
audience,
ad_name,
ad_id,
ad_status,
visual,
date,
date_granularity,
cost as spend,
impressions,
clicks,
complete_payment_events as purchases,
complete_payment_value as revenue,
conversions as bookings
FROM {{ ref('tiktok_performance_by_ad') }}
