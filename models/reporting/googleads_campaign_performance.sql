{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
CASE WHEN account_id = '3117773523' THEN 'Franchise'
    WHEN account_id = '8414555487' THEN 'Heyday Owned'
END as store_type,
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'Search_Brand' OR campaign_name ~* 'Heyday Brand' OR campaign_name ~* 'Search - Branded' THEN 'Campaign Type: Search Branded'
    ELSE 'Campaign Type: Search Nonbrand'
END as campaign_type_default,
CASE WHEN campaign_name ~* 'Chicago' OR campaign_name ~* 'LincolnPark' OR campaign_name ~* 'Lincoln Park' THEN 'Chicago'
    WHEN campaign_name ~* 'Denver' THEN 'Denver'
    WHEN campaign_name ~* 'NYC' THEN 'NYC'
    WHEN campaign_name ~* ' LA' OR campaign_name ~* '_LA' OR campaign_name ~* 'PlayaVista' OR campaign_name ~* 'Playa Vista' OR campaign_name ~* 'Manhattan Beach' OR campaign_name ~* 'ManhattanBeach' THEN 'LA'
    WHEN campaign_name ~* 'PHL' OR campaign_name ~* 'PlymouthMeeting' OR campaign_name ~* 'Plymouth Meeting' OR campaign_name ~* 'Rittenhouse' THEN 'PHL'
    WHEN campaign_name ~* 'Boston' OR campaign_name ~* 'Seaport' THEN 'Boston'
    WHEN campaign_name ~* 'AZ' OR campaign_name ~* 'San Tan' OR campaign_name ~* 'SanTan' OR campaign_name ~* 'Lincoln Plaza' OR campaign_name ~* 'LincolnPlaza' THEN 'AZ'
    WHEN campaign_name ~* 'ATL' OR campaign_name ~* 'Atlanta' THEN 'ATL'
    WHEN campaign_name ~* 'DMV' OR campaign_name ~* 'Bethesda' OR campaign_name ~* 'Alexandria' THEN 'DMV'
    WHEN campaign_name ~* 'Dallas' OR campaign_name ~* 'DFW' THEN 'Dallas'
    WHEN campaign_name ~* 'Houston' OR campaign_name ~* 'RiverOaks' OR campaign_name ~* 'River Oaks' THEN 'Houston'
    WHEN campaign_name ~* 'AnnArbor' OR campaign_name ~* 'Ann Arbor' THEN 'Ann Arbor'
    WHEN campaign_name ~* 'Plano' THEN 'Plano'
    WHEN campaign_name ~* 'East Cobb' THEN 'East Cobb'
END as market,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share,
heydayga4webfacial_booking as bookings
FROM {{ ref('googleads_performance_by_campaign') }}
