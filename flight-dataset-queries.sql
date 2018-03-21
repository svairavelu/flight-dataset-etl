--Write a query which returns the unique carriers
select distinct uniquecarrier from tracking;

--Return data for those 2003 flights with a delay of 5 hours or more
select count(*) from
(select tracking.*,
cast(case when arrdelay='NA' then '-9999' else arrdelay end as int ) arrdelay1,
cast(case when depdelay='NA' then '-9999' else depdelay end as int ) depdelay1
from tracking) t1
where year='2003' and (arrdelay1>300 or depdelay1>300);

--Return a count of the number of flights delayed by weather in each year
select year,count(*)
from tracking
where weatherdelay <> 'NA' and weatherdelay <> '0'
group by year;

--Determine the canceled flight ratio for each carrier in 2003
select cast(sum(case when cancelled='1' then 1 else 0 end) as float)
		/cast(count(*) as float)  as cancelled_flight_ratio
from tracking
where year=2003;

--Return the average arrival delay per month per destination per carrier in 2003
select month,uniquecarrier,dest,avg(arrdelay1) as average_arrival_delay
from
(select tracking.*,
cast(case when arrdelay='NA' then '-9999' else arrdelay end AS int ) arrdelay1
from tracking
where year=2003) t1
where arrdelay1 > 0
group by month,uniquecarrier,dest;

--Determine the 3 carriers with the highest average arrival delay per month per destination in 2003
select month,dest,uniquecarrier,average_arrival_delay, rnk from
(select  month,dest,uniquecarrier,average_arrival_delay,
rank() over (partition by month,dest order by average_arrival_delay desc) as rnk
from
(select month,uniquecarrier,dest,avg(arrdelay1) as average_arrival_delay
from
(select tracking.*,
cast(case when arrdelay='NA' then '-9999' else arrdelay end AS int ) arrdelay1
from tracking
where year=2003) t1
where arrdelay1 > 0
group by month,uniquecarrier,dest) a
) b where rnk<=3;
