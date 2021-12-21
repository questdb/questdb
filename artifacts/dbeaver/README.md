# PostgreSQL binary

Create new PostgreSQL database

```shell script
initdb -D "C:\pgdata"
```

Start PostgreSQL

```shell script
pg_ctl -D "C:\pgdata" start
```

Connect to PostgreSQL

```shell script
psql -d postgres
```

# SQLs that DBeaver runs from UI

## schemas

```sql
SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n
LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass
ORDER BY nspname;
select * from pg_catalog.pg_description d where d.classoid='pg_namespace'::regclass
```

## roles

```sql
SELECT a.oid,a.* FROM pg_catalog.pg_roles a
ORDER BY a.rolname;
```

## sessions

```sql
SELECT sa.* FROM pg_catalog.pg_stat_activity sa
```

## with every query!

```sql
SELECT current_schema(),session_user; -- DONE
```

then this one

```sql
SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n
LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass
WHERE nspname=$1 ORDER BY nspname
```

## extensions

```sql
SELECT
e.oid,
cfg.tbls,
n.nspname as schema_name,
e.*
FROM
pg_catalog.pg_extension e
join pg_namespace n on n.oid =e.extnamespace
left join  (
select
ARRAY_AGG(ns.nspname || '.' ||  cls.relname) tbls, oid_ext
from
(
select
unnest(e1.extconfig) oid , e1.oid oid_ext
from
pg_catalog.pg_extension e1 ) c
join    pg_class cls on cls.oid = c.oid
join pg_namespace ns on ns.oid = cls.relnamespace
group by oid_ext
) cfg on cfg.oid_ext = e.oid
ORDER BY e.oid;
```

## storage

```sql
SELECT t.oid,t.*,pg_tablespace_location(t.oid) loc
FROM pg_catalog.pg_tablespace t
ORDER BY t.oid;
```

## locks

```sql
with locks as ( select  pid,locktype, mode,granted,transactionid tid,relation,page,tuple from pg_locks ), conflict as ( select *  from (values ('AccessShareLock','AccessExclusiveLock',1), ('RowShareLock','ExclusiveLock',1), ('RowShareLock','AccessExclusiveLock',2),        ('RowExclusiveLock','ShareLock', 1), ('RowExclusiveLock','ShareRowExclusiveLock',2),  ('RowExclusiveLock','ExclusiveLock',3), ('RowExclusiveLock','AccessExclusiveLock',4), ('ShareUpdateExclusiveLock','ShareUpdateExclusiveLock',1), ('ShareUpdateExclusiveLock','ShareLock',2),  ('ShareUpdateExclusiveLock','ShareRowExclusiveLock',3), ('ShareUpdateExclusiveLock','ExclusiveLock', 4), ('ShareUpdateExclusiveLock','AccessExclusiveLock',5), ('ShareLock','RowExclusiveLock',1),  ('ShareLock','ShareUpdateExclusiveLock',2),  ('ShareLock','ShareRowExclusiveLock',3),  ('ShareLock','ExclusiveLock',4),	   ('ShareLock','AccessExclusiveLock',5), ('ShareRowExclusiveLock','RowExclusiveLock', 1),  ('ShareRowExclusiveLock','ShareUpdateExclusiveLock',    2),  ('ShareRowExclusiveLock','ShareLock',    3),  ('ShareRowExclusiveLock','ShareRowExclusiveLock',4),  ('ShareRowExclusiveLock','ExclusiveLock',5),  ('ShareRowExclusiveLock','AccessExclusiveLock', 6), ('ExclusiveLock','RowShareLock',1), ('ExclusiveLock','RowExclusiveLock',2), ('ExclusiveLock','ShareUpdateExclusiveLock',3),  ('ExclusiveLock','ShareLock',4),  ('ExclusiveLock','ShareRowExclusiveLock',5),   ('ExclusiveLock','ExclusiveLock',6),   ('ExclusiveLock','AccessExclusiveLock',7), ('AccessExclusiveLock','AccessShareLock',1), ('AccessExclusiveLock','RowShareLock',2), ('AccessExclusiveLock','RowExclusiveLock',3), ('AccessExclusiveLock','ShareUpdateExclusiveLock',4),   ('AccessExclusiveLock','ShareLock',5), ('AccessExclusiveLock','ShareRowExclusiveLock',6), ('AccessExclusiveLock','ExclusiveLock',7),  ('AccessExclusiveLock','AccessExclusiveLock',8) ) as t (mode1,mode2,prt)     )	  ,real_locks as (select 	  la.pid as blocked_pid, blocked_activity.usename  AS blocked_user, la.blocked     AS blocking_pid, blocking_activity.usename AS blocking_user, blocked_activity.query    AS blocked_statement, blocking_activity.query   AS statement_in from  ( 	select 				 l.*, c.mode2, c.prt, l2.pid blocked, row_number() over(partition by l.pid order by c.prt) rid from   locks l join conflict c on l.mode = c.mode1 join locks l2 on l2.locktype = l.locktype and l2.mode = c.mode2 and l2.granted and l.pid != l2.pid and  coalesce(l.tid::text,'*') ||':'|| coalesce(l.relation::text,'*') ||':'|| coalesce(l.page::text,'*') ||':'|| coalesce(l.tuple::text,'*') = coalesce(l2.tid::text,'*') ||':'|| coalesce(l2.relation::text,'*') ||':'|| coalesce(l2.page::text,'*') ||':'|| coalesce(l2.tuple::text,'*') where not l.granted ) la join pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = la.pid join pg_catalog.pg_stat_activity blocking_activity  ON blocking_activity.pid = la.blocked where la.rid = 1) , root_quest as (    select blocking_pid as blocking_pid from real_locks  except  select blocked_pid from real_locks )  select blocked_pid,        blocked_user,       blocking_pid,      blocking_user,     blocked_statement,    statement_in  from  real_locks union select real_locks.blocking_pid,  real_locks.blocking_user,  null::integer,  null::text,  real_locks.statement_in,  null::text  from real_locks,  root_quest  where real_locks.blocking_pid = root_quest.blocking_pid;
```
