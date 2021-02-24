with agents as (select u.id agent_id,u.created_at::date as date_debut,u.username, concat(u.first_name,' ',u.last_name) as agent,r.name as agent_role,u.role_id,zone_id,z.name as zone,id_tenant from users u
inner join(select * from zones)z on u.zone_id=z.id
inner join(select * from roles)r on r.id = u.role_id
where u.is_active is True and blocked is False and u.role_id in(11,18,5,14,4))
select a.*,superviseur,sup_username,sup_id from agents a
inner join(select agent as superviseur,username as sup_username,agent_id as sup_id, zone_id as sup_zone_id from agents where role_id=4)aa on a.zone_id = aa.sup_zone_id
where id_tenant !=3 and a.role_id!=4
-- limit 100