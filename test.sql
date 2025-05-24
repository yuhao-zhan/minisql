create database db0;
show databases;
use db0;

create table t1(a int, b char(20) unique, c float, primary key(a, c));
create table t1(a int, b char(0) unique, c float, primary key(a, c));
create table t1(a int, b char(-5) unique, c float, primary key(a, c));
create table t1(a int, b char(3.69) unique, c float, primary key(a, c));
create table t1(a int, b char(-0.69) unique, c float, primary key(a, c));

create table student(
  sno char(8),
  sage int,
  sab float unique,
  primary key (sno, sab)
);

drop table t1;

create table t1(a int, b char(20), c float);
create index idx1 on t1(a, b);
create index idx1 on t1(a, b) using btree;
show indexes;
drop index idx1;

insert into t1 values(1, "aaa", null);
select * from t1;
select a, b from t1;
select * from t1 where a = 1;
select * from t1 where a = 1 and b = "aaa";
select * from t1 where a = 1 and b = "aaa" or c is null and b not null;

update t1 set c = 3;
update t1 set a = 1, b = "ccc" where b = 2.33;
delete from t1;
delete from t1 where a = 1 and c = 2.33;
