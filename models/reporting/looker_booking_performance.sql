{{ config (
    alias = target.database + '_looker_booking_performance'
)}}

SELECT DATE_TRUNC('day',created_date::date) as date, 'day' as date_granularity,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side|Silver Lake|Playa Vista|Beverly|Manhattan Beach|Rittenhouse|Lincoln Plaza|Midtown|Krog District|Buckhead|Perimeter|Bethesda Row|14th St|Old Town Alexandria|Preston Hollow|Knox Henderson') 
            THEN 'Heyday Owned'
            WHEN location_name ~* ('Ann Arbor|Santan Village|Seaport|Lincoln Park|Lowry|Tennyson|University Hills|River Oaks|Plymouth Meeting') THEN 'Franchise'
        END as store_type,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side') THEN 'NYC'
            WHEN location_name ~* ('Silver Lake|Playa Vista|Beverly|Manhattan Beach') THEN 'LA'
            WHEN location_name ~* ('Rittenhouse|Plymouth Meeting') THEN 'PHL'
            WHEN location_name ~* ('Lincoln Plaza|Santan Village') THEN 'AZ'
            WHEN location_name ~* ('Midtown|Krog District|Buckhead|Perimeter') THEN 'ATL'
            WHEN location_name ~* ('Bethesda Row|14th St|Old Town Alexandria') THEN 'DMV'
            WHEN location_name ~* ('Preston Hollow|Knox Henderson') THEN 'Dallas'
            WHEN location_name ~* ('Ann Arbor') THEN 'Ann Arbor'
            WHEN location_name ~* ('Seaport') THEN 'Boston'
            WHEN location_name ~* ('Lincoln Park') THEN 'Chicago'
            WHEN location_name ~* ('Lowry|Tennyson|University Hills') THEN 'Denver'
            WHEN location_name ~* ('River Oaks') THEN 'Houston'
        END as market,
        location_name,
        client_type,
        COALESCE(SUM(appointments),0) as bookings
    FROM {{ ref('s3_bookings') }}
    GROUP BY 1,2,3,4,5,6

UNION ALL

        SELECT DATE_TRUNC('week',created_date::date) as date, 'week' as date_granularity,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side|Silver Lake|Playa Vista|Beverly|Manhattan Beach|Rittenhouse|Lincoln Plaza|Midtown|Krog District|Buckhead|Perimeter|Bethesda Row|14th St|Old Town Alexandria|Preston Hollow|Knox Henderson') 
            THEN 'Heyday Owned'
            WHEN location_name ~* ('Ann Arbor|Santan Village|Seaport|Lincoln Park|Lowry|Tennyson|University Hills|River Oaks|Plymouth Meeting') THEN 'Franchise'
        END as store_type,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side') THEN 'NYC'
            WHEN location_name ~* ('Silver Lake|Playa Vista|Beverly|Manhattan Beach') THEN 'LA'
            WHEN location_name ~* ('Rittenhouse|Plymouth Meeting') THEN 'PHL'
            WHEN location_name ~* ('Lincoln Plaza|Santan Village') THEN 'AZ'
            WHEN location_name ~* ('Midtown|Krog District|Buckhead|Perimeter') THEN 'ATL'
            WHEN location_name ~* ('Bethesda Row|14th St|Old Town Alexandria') THEN 'DMV'
            WHEN location_name ~* ('Preston Hollow|Knox Henderson') THEN 'Dallas'
            WHEN location_name ~* ('Ann Arbor') THEN 'Ann Arbor'
            WHEN location_name ~* ('Seaport') THEN 'Boston'
            WHEN location_name ~* ('Lincoln Park') THEN 'Chicago'
            WHEN location_name ~* ('Lowry|Tennyson|University Hills') THEN 'Denver'
            WHEN location_name ~* ('River Oaks') THEN 'Houston'
        END as market,
        location_name,
        client_type,
        COALESCE(SUM(appointments),0) as bookings
    FROM {{ ref('s3_bookings') }}
    GROUP BY 1,2,3,4,5,6

UNION ALL

        SELECT DATE_TRUNC('month',created_date::date) as date, 'month' as date_granularity,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side|Silver Lake|Playa Vista|Beverly|Manhattan Beach|Rittenhouse|Lincoln Plaza|Midtown|Krog District|Buckhead|Perimeter|Bethesda Row|14th St|Old Town Alexandria|Preston Hollow|Knox Henderson') 
            THEN 'Heyday Owned'
            WHEN location_name ~* ('Ann Arbor|Santan Village|Seaport|Lincoln Park|Lowry|Tennyson|University Hills|River Oaks|Plymouth Meeting') THEN 'Franchise'
        END as store_type,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side') THEN 'NYC'
            WHEN location_name ~* ('Silver Lake|Playa Vista|Beverly|Manhattan Beach') THEN 'LA'
            WHEN location_name ~* ('Rittenhouse|Plymouth Meeting') THEN 'PHL'
            WHEN location_name ~* ('Lincoln Plaza|Santan Village') THEN 'AZ'
            WHEN location_name ~* ('Midtown|Krog District|Buckhead|Perimeter') THEN 'ATL'
            WHEN location_name ~* ('Bethesda Row|14th St|Old Town Alexandria') THEN 'DMV'
            WHEN location_name ~* ('Preston Hollow|Knox Henderson') THEN 'Dallas'
            WHEN location_name ~* ('Ann Arbor') THEN 'Ann Arbor'
            WHEN location_name ~* ('Seaport') THEN 'Boston'
            WHEN location_name ~* ('Lincoln Park') THEN 'Chicago'
            WHEN location_name ~* ('Lowry|Tennyson|University Hills') THEN 'Denver'
            WHEN location_name ~* ('River Oaks') THEN 'Houston'
        END as market,
        location_name,
        client_type,
        COALESCE(SUM(appointments),0) as bookings
    FROM {{ ref('s3_bookings') }}
    GROUP BY 1,2,3,4,5,6
    
UNION ALL

        SELECT DATE_TRUNC('quarter',created_date::date) as date, 'quarter' as date_granularity,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side|Silver Lake|Playa Vista|Beverly|Manhattan Beach|Rittenhouse|Lincoln Plaza|Midtown|Krog District|Buckhead|Perimeter|Bethesda Row|14th St|Old Town Alexandria|Preston Hollow|Knox Henderson') 
            THEN 'Heyday Owned'
            WHEN location_name ~* ('Ann Arbor|Santan Village|Seaport|Lincoln Park|Lowry|Tennyson|University Hills|River Oaks|Plymouth Meeting') THEN 'Franchise'
        END as store_type,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side') THEN 'NYC'
            WHEN location_name ~* ('Silver Lake|Playa Vista|Beverly|Manhattan Beach') THEN 'LA'
            WHEN location_name ~* ('Rittenhouse|Plymouth Meeting') THEN 'PHL'
            WHEN location_name ~* ('Lincoln Plaza|Santan Village') THEN 'AZ'
            WHEN location_name ~* ('Midtown|Krog District|Buckhead|Perimeter') THEN 'ATL'
            WHEN location_name ~* ('Bethesda Row|14th St|Old Town Alexandria') THEN 'DMV'
            WHEN location_name ~* ('Preston Hollow|Knox Henderson') THEN 'Dallas'
            WHEN location_name ~* ('Ann Arbor') THEN 'Ann Arbor'
            WHEN location_name ~* ('Seaport') THEN 'Boston'
            WHEN location_name ~* ('Lincoln Park') THEN 'Chicago'
            WHEN location_name ~* ('Lowry|Tennyson|University Hills') THEN 'Denver'
            WHEN location_name ~* ('River Oaks') THEN 'Houston'
        END as market,
        location_name,
        client_type,
        COALESCE(SUM(appointments),0) as bookings
    FROM {{ ref('s3_bookings') }}
    GROUP BY 1,2,3,4,5,6
    
UNION ALL

        SELECT DATE_TRUNC('year',created_date::date) as date, 'year' as date_granularity,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side|Silver Lake|Playa Vista|Beverly|Manhattan Beach|Rittenhouse|Lincoln Plaza|Midtown|Krog District|Buckhead|Perimeter|Bethesda Row|14th St|Old Town Alexandria|Preston Hollow|Knox Henderson') 
            THEN 'Heyday Owned'
            WHEN location_name ~* ('Ann Arbor|Santan Village|Seaport|Lincoln Park|Lowry|Tennyson|University Hills|River Oaks|Plymouth Meeting') THEN 'Franchise'
        END as store_type,
        CASE WHEN location_name ~* ('19th St|Nomad|Noho|Tribeca|UES|Upper East Side|UWS|Upper West Side') THEN 'NYC'
            WHEN location_name ~* ('Silver Lake|Playa Vista|Beverly|Manhattan Beach') THEN 'LA'
            WHEN location_name ~* ('Rittenhouse|Plymouth Meeting') THEN 'PHL'
            WHEN location_name ~* ('Lincoln Plaza|Santan Village') THEN 'AZ'
            WHEN location_name ~* ('Midtown|Krog District|Buckhead|Perimeter') THEN 'ATL'
            WHEN location_name ~* ('Bethesda Row|14th St|Old Town Alexandria') THEN 'DMV'
            WHEN location_name ~* ('Preston Hollow|Knox Henderson') THEN 'Dallas'
            WHEN location_name ~* ('Ann Arbor') THEN 'Ann Arbor'
            WHEN location_name ~* ('Seaport') THEN 'Boston'
            WHEN location_name ~* ('Lincoln Park') THEN 'Chicago'
            WHEN location_name ~* ('Lowry|Tennyson|University Hills') THEN 'Denver'
            WHEN location_name ~* ('River Oaks') THEN 'Houston'
        END as market,
        location_name,
        client_type,
        COALESCE(SUM(appointments),0) as bookings
    FROM {{ ref('s3_bookings') }}
    GROUP BY 1,2,3,4,5,6
