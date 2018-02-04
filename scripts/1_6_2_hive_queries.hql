use emirates_db;

select b.Description, a.UniqueCarrier,round(cast(((totalCount - (cancelCount+delayCount+divertedCount))*100/totalCount) as float), 2) as onTimePerformance, totalCount from
    (
        select UniqueCarrier, count(*) as totalCount, sum(Cancelled) as cancelCount, sum(case when ArrDelay > 0 then 1 else 0 end) as delayCount, sum(Diverted) as divertedCount from ht_otp a group by UniqueCarrier
    ) a
left join
ht_carriers b
on concat("\"",a.UniqueCarrier, "\"") = cast(b.Code as string)
where Description is not NULL
order by onTimePerformance desc
limit 1;



select Year, Month, DayofMonth, round(cast(((totalCount - (cancelCount+delayCount+divertedCount))*100/totalCount) as float), 2) as onTimePerformance, totalCount from
    (
        select Year, Month, DayofMonth, count(*) as totalCount, sum(Cancelled) as cancelCount, sum(case when ArrDelay > 0 then 1 else 0 end) as delayCount, sum(Diverted) as divertedCount from ht_otp a group by Year, Month, DayofMonth
    ) a
where Year is not NULL
order by onTimePerformance desc
limit 1;



select DayOfWeek, round(cast(((totalCount - (cancelCount+delayCount+divertedCount))*100/totalCount) as float), 2) as onTimePerformance, totalCount from
    (
        select DayOfWeek, count(*) as totalCount, sum(Cancelled) as cancelCount, sum(case when ArrDelay > 0 then 1 else 0 end) as delayCount, sum(Diverted) as divertedCount from ht_otp a group by DayOfWeek
    ) a
where DayOfWeek is not NULL
order by onTimePerformance desc
limit 1;



select startHrs, cast((startHrs+1) as int) as endHrs, round(cast(((totalCount - (cancelCount+delayCount+divertedCount))*100/totalCount) as float), 2) as onTimePerformance, totalCount from
    (
        select startHrs, count(*) as totalCount, sum(Cancelled) as cancelCount, sum(case when ArrDelay > 0 then 1 else 0 end) as delayCount, sum(Diverted) as divertedCount from
            (
                select cast((CRSDepTime - pmod(CRSDepTime, 100)) as int) as startHrs,Cancelled, ArrDelay, Diverted from ht_otp a
            ) b group by startHrs
    ) c
order by onTimePerformance desc
limit 1;




select b.issue_date, a.TailNum, round(cast(((totalCount - (cancelCount+delayCount+divertedCount))*100/totalCount) as float), 2) as onTimePerformance, totalCount from
    (
        select TailNum, count(*) as totalCount, sum(Cancelled) as cancelCount, sum(case when ArrDelay > 0 then 1 else 0 end) as delayCount, sum(Diverted) as divertedCount from ht_otp a group by TailNum
    ) a
left join
    (
        select tailnum, issue_date from ht_planedate
    ) b
on a.TailNum = b.tailnum
where issue_date is not NULL
order by onTimePerformance asc
limit 10;




select t2.originCode, t2.originPerformance, t2.originairport, t1.destCode, t1.destairportname, t1.destperformance from
(
    select e.Dest as destCode, e.Origin as originCompare, ee.airportName as destairportname, e.onTimePerformance as destperformance from
    (
        select Dest, Origin, round(cast(((totalCount - (cancelCount+delayCount+divertedCount))*100/totalCount) as float), 2) as onTimePerformance from
        (
            select Dest, Origin, count(*) as totalCount, sum(Cancelled) as cancelCount, sum(case when ArrDelay > 0 then 1 else 0 end) as delayCount, sum(Diverted) as divertedCount from ht_otp aa group by Dest, Origin
        ) d
    ) e
    left join
    (
        select iata as destairport, airport as airportName from ht_aiports
    )ee
on ee.destairport = concat("\"",e.Dest, "\"")
) t1

left join

(
    select c.Dest as originCode, c.onTimePerformance as originPerformance, cc.airportName as originairport from
    (
        select Dest, round(cast(((totalCount - (cancelCount+delayCount+divertedCount))*100/totalCount) as float), 2) as onTimePerformance from
        (
            select Dest, count(*) as totalCount, sum(Cancelled) as cancelCount, sum(case when ArrDelay > 0 then 1 else 0 end) as delayCount, sum(Diverted) as divertedCount from ht_otp a group by Dest
        ) b
    ) c
    left join
    (
        select iata as destairport, airport as airportName from ht_aiports
    )cc
    on cc.destairport = concat("\"",c.Dest, "\"")
) t2
on t2.originCode = t1.originCompare
where t2.originCode is not NULL or
t1.destCode is not NULL
order by t2.originPerformance desc, t1.destperformance desc
limit 10
;

