select s.name as stock_name,stock_type,us.user_id as sup_id,u.username,z.name as zone,
concat(u.first_name,' ',last_name) as superviseur,u.id_tenant,count(sp.product_id) stock --,p.serial

from stocks s,users_stocks us, users u,stocks_products sp,zones z
where s.id = us.stock_id and u.id = us.user_id and sp.stock_id = s.id and z.id=u.zone_id and s.deleted is False 
and u.is_active is True and u.blocked = False and u.role_id=4
group by 1,2,3,4,5,6,7
 limit 100