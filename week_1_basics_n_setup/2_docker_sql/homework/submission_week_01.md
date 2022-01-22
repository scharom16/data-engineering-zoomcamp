# Submission for the first week

## Question 3

How many taxi trips were there on January 15?

``` sql
select
    count(*)
from
    yellow_taxi_trips t
where
    t."tpep_pickup_datetime" between '2021-01-15' and '2021-01-16'
```

## Question 4

On which day it was the largest tip in January?

``` sql
select
    t."tpep_dropoff_datetime"
from
    yellow_taxi_trips t
where
    to_char(t."tpep_dropoff_datetime", 'YYYY-MM-DD') in ('2021-01-20', '2021-01-04', '2021-01-01', '2021-01-21')
order by
    t."tip_amount" DESC
limit 1
```

## Question 5

What was the most popular destination for passengers picked up in central park on January 14? Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

``` sql
select 
    z."Zone"
from
 zones z,
 yellow_taxi_trips t
where
 z."LocationID" = (
 select "DOLocationID"
    from yellow_taxi_trips
    group by "DOLocationID"
    order by count(*) DESC
    LIMIT(1)
 )
 limit(1)
```

## Question 6

What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)? Enter two zone names separated by a slashFor example:"Jamaica Bay / Clinton East"If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

``` sql
select avg("total_amount") avg_total_amount, z1."Zone", z2."Zone"
from
    yellow_taxi_trips a,
    zones z2,
    zones z1
where
    a."PULocationID" = z1."LocationID"
    and
    a."DOLocationID" = z2."LocationID"
group by
    z1."Zone", z2."Zone"
order by avg_total_amount desc
limit 1

```
