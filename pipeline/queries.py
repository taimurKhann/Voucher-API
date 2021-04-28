staging_table = "customer_voucher_staging"
frequent_segment_table = "frequent_segment_final"
recency_segment_table = "recency_segment_final"

create_table_query = """
    DROP TABLE IF EXISTS customer_voucher_staging;

    CREATE TABLE customer_voucher_staging (
        id int PRIMARY KEY,
        timestamp varchar not null,
        country_code varchar not null,
        last_order_ts varchar not null,
        first_order_ts varchar not null,
        total_orders varchar not null,
        voucher_amount varchar not null
    );

    DROP TABLE IF EXISTS customer_voucher_final;
    
    CREATE TABLE customer_voucher_final (
	    event_time timestamp NOT NULL,
	    country_code varchar NOT NULL,
	    last_order_ts timestamp NOT NULL,
	    first_order_ts timestamp NOT NULL,
	    total_orders float NOT NULL,
	    voucher_amount float NOT NULL,
	    days_since_last_order int NOT NULL,
	    order_bracket varchar,
	    recency_bracket varchar 
    );
    """

customer_voucher_transformation_query = """
    insert into customer_voucher_final 
    with customer_voucher_filter as (
    	select 
    		"timestamp" :: "timestamp" as event_time,
    		country_code,
    		last_order_ts :: "timestamp",
    		first_order_ts :: "timestamp",
    		total_orders :: float,
    		voucher_amount ::float
    	from 
    		customer_voucher_staging
    	where
    		country_code IN ('Peru')
    	and total_orders != ''
    	and voucher_amount != ''
    ), customer_voucher_segment_feature as (
    	select 
    		*,
    		DATE_PART('day', event_time - last_order_ts)::int as days_since_last_order
    	from 
    		customer_voucher_filter
    )
    select 
    	*,
    	case
    		when total_orders between 0 and 4 then '0-4'
    		when total_orders between 5 and 13 then '5-13'
    		when total_orders between 14 and 37 then '14-37'
    		else null
    	end as order_bracket,
    	case
    		when days_since_last_order between 30 and 60 then '30-60'
    		when days_since_last_order between 61 and 90 then '61-90'
    		when days_since_last_order between 91 and 120 then '91-120'
    		when days_since_last_order between 121 and 180 then '121-180'
    		when days_since_last_order > 180 then '180+'
    		else null
    	end as recency_bracket
    from 
    	customer_voucher_segment_feature;
    """

frequent_segment_query = """
    drop table if exists frequent_segment_final;
    
    create table frequent_segment_final as
    SELECT 
        country_code
      , order_bracket
      , MODE() within group (order by voucher_amount) voucher_amount
    FROM 
    	customer_voucher_final
    where 
    	order_bracket is not null
    GROUP by
    	country_code
      , order_bracket;
    """

recency_segment_query = """
    drop table if exists recency_segment_final;
    
    create table recency_segment_final as
    SELECT 
        country_code
      , recency_bracket
      , MODE() within group (order by voucher_amount)  voucher_amount
    FROM 
    	customer_voucher_final
    where 
    	recency_bracket is not null
    GROUP by
    	country_code
      , recency_bracket;
    """

voucher_amount_query = """
    select 
    	voucher_amount 
    from
    	{}
    where
    	country_code = '{}'
    and {} = '{}';
"""