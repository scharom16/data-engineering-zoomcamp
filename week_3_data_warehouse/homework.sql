Select count(*) From `dtc-de-course-338814.trips_data_all.fhv_tripdata` 
Where EXTRACT(YEAR FROM pickup_datetime) =  2019

Select count(distinct(dispatching_base_num)) From `dtc-de-course-338814.trips_data_all.fhv_tripdata` 
Where EXTRACT(YEAR FROM pickup_datetime) =  2019

Select count(*) From `dtc-de-course-338814.trips_data_all.fhv_tripdata` 
Where pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31'
AND
dispatching_base_num in ('B00987', 'B02060', 'B02279')

Select count(distinct(dispatching_base_num)), count(distinct(SR_Flag)) From `dtc-de-course-338814.trips_data_all.fhv_tripdata`