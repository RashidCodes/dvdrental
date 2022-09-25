drop table if exists {{ target_table }}; 

create table {{ target_table }} as (
    select 
        film_id, 
        title,
        category_name, 
        last_value(title) over (
            partition by category_id 
            order by rental_count
            rows between unbounded preceding and unbounded following 
            -- we need to specify unbounded preceding and following because the frame_end defaults to the current row 
        ) most_popular_film_in_category
    from 
        staging_films 
    order by category_name 
); 