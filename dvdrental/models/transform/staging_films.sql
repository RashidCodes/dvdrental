drop table if exists {{ target_table }}; 

create table {{ target_table }} as (
    with films as (
        select 
            f.film_id, 
            f.title,
            f.release_year, 
            c.category_id, 
            c.name as category_name,
            sum(p.amount) as sales,
            count(r.*) rental_count
        from 
            film f inner join film_category fc 
                on f.film_id = fc.film_id 
            inner join category as c 
                on c.category_id = fc.category_id 
            inner join inventory i 
                on i.film_id = f.film_id 
            inner join rental r 
                on r.inventory_id = i.inventory_id 
            inner join payment as p 
                on p.rental_id = r.rental_id 
        group by 
            f.film_id, 
            c.category_id, 
            f.title,
            f.release_year, 
            c.category_id, 
            category_name
    )

    select 
        *
    from films
); 