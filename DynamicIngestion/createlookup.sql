create table if not exists databricksinterview.source.filelookup
(
  file string,
  type string

);
insert into databricksinterview.source.filelookup 
values('orders_day1','csv'),('productsOrders','csv');




