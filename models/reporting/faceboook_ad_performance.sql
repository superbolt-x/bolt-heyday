{{ config (
    alias = target.database + '_faceboook_ad_performance'
)}}
  
SELECT 
CASE WHEN account_id = '620149775615288' THEN 'Franchise'
    WHEN account_id = '10100119278651428' THEN 'Heyday Owned'
END as store_type,
campaign_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name ~* 'Prosp' OR campaign_name ~* 'Purchase' THEN 'Campaign Type: Prospecting'
    WHEN campaign_name ~* 'Rmkt' OR campaign_name ~* 'Retargeting' THEN 'Campaign Type: Retargeting'
END as campaign_type_default,
CASE WHEN campaign_name ~* 'Chicago' OR campaign_name ~* 'LincolnPark' OR campaign_name ~* 'Lincoln Park' THEN 'Chicago'
    WHEN campaign_name ~* 'Denver' THEN 'Denver'
    WHEN campaign_name ~* 'NYC' THEN 'NYC'
    WHEN campaign_name ~* 'LA' OR campaign_name ~* 'PlayaVista' OR campaign_name ~* 'Playa Vista' OR campaign_name ~* 'Manhattan Beach' OR campaign_name ~* 'ManhattanBeach'THEN 'LA'
    WHEN campaign_name ~* 'PHL' OR campaign_name ~* 'PlymouthMeeting' OR campaign_name ~* 'Plymouth Meeting' OR campaign_name ~* 'Rittenhouse' THEN 'PHL'
    WHEN campaign_name ~* 'Boston' OR campaign_name ~* 'Seaport' THEN 'Boston'
    WHEN campaign_name ~* 'AZ' OR campaign_name ~* 'San Tan' OR campaign_name ~* 'SanTan' OR campaign_name ~* 'Lincoln Plaza' OR campaign_name ~* 'LincolnPlaza' THEN 'AZ'
    WHEN campaign_name ~* 'ATL' OR campaign_name ~* 'Atlanta' THEN 'ATL'
    WHEN campaign_name ~* 'DMV' OR campaign_name ~* 'Bethesda' OR campaign_name ~* 'Alexandria' THEN 'DMV'
    WHEN campaign_name ~* 'Dallas' THEN 'Dallas'
    WHEN campaign_name ~* 'Houston' OR campaign_name ~* 'RiverOaks' OR campaign_name ~* 'River Oaks' THEN 'Houston'
    WHEN campaign_name ~* 'AnnArbor' OR campaign_name ~* 'Ann Arbor' THEN 'Ann Arbor'
END as market,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue,
"offsite_conversion.custom.1549904315410411" as bookings
FROM {{ ref('reporting','facebook_performance_by_ad') }}
