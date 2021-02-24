select s.name as stock_name,stock_type,us.user_id as agent_id ,pt.name as product_type,pt.category,
       count(p.serial)::int as stock
from users_stocks us,stocks s,stocks_products sp,products p,product_types pt
where s.id = us.stock_id and sp.stock_id = s.id and p.id = sp.product_id
and pt.id = p.product_type_id and s.deleted is False
group by 1,2,3,4,5