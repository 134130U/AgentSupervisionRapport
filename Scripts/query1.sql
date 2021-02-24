with agents as (select u.id agent_id, concat(u.first_name,' ',u.last_name) as agent,r.name as agent_role,u.role_id,zone_id,z.name as zone,id_tenant from users u
inner join(select * from zones)z on u.zone_id=z.id
inner join(select * from roles)r on r.id = u.role_id
where u.is_active is True and blocked is False and u.role_id in(11,18,5,14,4)),
agent_stock as 
(select s.name as stock_name,stock_type,us.user_id as agent_id,--count(sp.product_id) stock ,
pt.name as product_type,pt.category,count(p.serial) as stock
from stocks s,users_stocks us, users u,stocks_products sp,products p,product_types pt
where s.id = us.stock_id and u.id = us.user_id and sp.stock_id = s.id and p.id = sp.product_id
and pt.id = p.product_type_id and s.deleted is False 
and u.is_active is True and u.blocked = False and u.role_id!=4
group by 1,2,3,4,5)
select a.agent_id,a.agent,agent_role,role_id,zone_id,zone,id_tenant,superviseur,sup_id,sup_zone_id,stock_name,
product_type,category,stock from agents a
inner join(select agent as superviseur,agent_id as sup_id, zone_id as sup_zone_id from agents where role_id=4)aa on a.zone_id = aa.sup_zone_id
inner join(select * from agent_stock)sa on sa.agent_id = a.agent_id and id_tenant!=3

-- limit 100