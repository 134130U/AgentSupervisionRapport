select a.created_at::date,to_char(a.created_at,'yyyy')::int as annee, to_char(a.created_at,'mm')::int as mois,
       to_char(a.created_at,'dd')::int as jour,u.id as agent_id,u.zone_id,concat(u.first_name,' ',u.last_name) as agent,
       u.id_tenant,g.name as group_prix,case when a.arrhes isnull then a.total_amount else a.arrhes end as deposit ,count(a.slug) as ventes
from users u
inner join (select * from accounts) a on a.user_id = u.id
inner join (select * from groups) g on g.id = a.group_id
where u.is_active is True and blocked is False and u.role_id in(11,18,5,14) and u.id_tenant!=3
and u.id !=6 and a.status !=6 and a.created_at > now()::date - interval '2 months'
group by 1,2,3,4,5,6,7,8,9,10
-- order by 1