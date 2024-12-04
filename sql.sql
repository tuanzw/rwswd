select t.clock_event_type, t.payroll_id, t.timezone, t.datetime from
    (select last_name as payroll_id, 'IN' clock_event_type, 'Asia/Jarkata' timezone, to_char(first_in_time ,'yyyy-mm-ddThh24:mi:ss') datetime from acc_firstin_lastout
    where to_char(create_time,'yyyymmdd') = :wdate
    union all 
    select last_name as payroll_id, 'OUT' clock_event_type, 'Asia/Jarkata' timezone, to_char(last_out_time ,'yyyy-mm-ddThh24:mi:ss') datetime from acc_firstin_lastout
    where to_char(create_time,'yyyymmdd') = :wdate) t
where t.payroll_id is not null
order by t.payroll_id, t.clock_event_type

--UAT below
/*select t.empid as payroll_id, t.clock_event_type, t.timezone, t.datetime from (
    select empid, 'IN' clock_event_type, 'Asia/Jarkata' timezone, to_char(createdat,'yyyy-mm-ddThh24:mi:ss') datetime from t_assignment
    union all 
    select empid, 'OUT' clock_event_type, 'Asia/Jarkata' timezone, to_char(updatedat,'yyyy-mm-ddThh24:mi:ss') datetime from t_assignment
) t
where :wdate = :wdate
order by t.empid, t.clock_event_type
*/