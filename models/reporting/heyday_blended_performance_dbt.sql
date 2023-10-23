SELECT 'Facebook - Prospecting' as channel, date, date_granularity, store_type, market, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(spend),0) as pros_spend, COALESCE(SUM(link_clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(purchases),0) as pros_purchases, COALESCE(SUM(bookings),0) as paid_bookings, 
        0 as bookings, 0 as new_bookings
    FROM {{ source('reporting','heyday_facebook_ad_performance') }}
    WHERE campaign_type_default ~* 'Prospecting'
    GROUP BY 1,2,3,4,5
    
    UNION ALL
    
    SELECT 'Facebook - Retargeting' as channel, date, date_granularity, store_type, market, 
        COALESCE(SUM(spend),0) as spend, 0 as pros_spend, COALESCE(SUM(link_clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as purchases, 0 as pros_purchases, COALESCE(SUM(bookings),0) as paid_bookings, 
        0 as bookings, 0 as new_bookings
    FROM {{ source('reporting','heyday_facebook_ad_performance') }}
    WHERE campaign_type_default ~* 'Retargeting'
    GROUP BY 1,2,3,4,5
    
    UNION ALL
    
    SELECT 'Google - Branded' as channel, date, date_granularity, store_type, market, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(spend),0) as pros_spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(purchases),0) as pros_purchases, COALESCE(SUM(bookings),0) as paid_bookings, 
        0 as bookings, 0 as new_bookings
    FROM {{ source('reporting','heyday_googleads_campaign_performance') }}
    WHERE campaign_type_default ~* 'Search Branded'
    GROUP BY 1,2,3,4,5
    
    UNION ALL
    
    SELECT 'Google - Nonbrand' as channel, date, date_granularity, store_type, market, 
        COALESCE(SUM(spend),0) as spend, 0 as pros_spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as purchases, 0 as pros_purchases, COALESCE(SUM(bookings),0) as paid_bookings, 
        0 as bookings, 0 as new_bookings
    FROM {{ source('reporting','heyday_googleads_campaign_performance') }}
    WHERE campaign_type_default ~* 'Search Nonbrand'
    GROUP BY 1,2,3,4,5
    
    UNION ALL
    
    SELECT 'TikTok - Prospecting' as channel, date, date_granularity, store_type, market, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(spend),0) as pros_spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(purchases),0) as pros_purchases, COALESCE(SUM(bookings),0) as paid_bookings, 
        0 as bookings, 0 as new_bookings
    FROM {{ source('reporting','heyday_tiktok_ad_performance') }}
    GROUP BY 1,2,3,4,5
    
    UNION ALL
    
    SELECT 'Looker' as channel, date, date_granularity, store_type, market, 
        0 as spend, 0 as pros_spend, 0 as clicks, 0 as impressions, 0 as purchases, 0 as pros_purchases, 0 as paid_bookings, 
        COALESCE(SUM(bookings),0) as bookings, COALESCE(SUM(CASE WHEN client_type ~* 'New Client' THEN bookings END),0) as new_bookings
    FROM {{ source('reporting','heyday_looker_booking_performance') }}
    GROUP BY 1,2,3,4,5
