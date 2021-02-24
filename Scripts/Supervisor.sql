select u.id agent_id,u.username sup_username, concat(u.first_name,' ',u.last_name) as superviseur,z.name as zone,id_tenant from users u
inner join(select * from zones)z on u.zone_id=z.id
inner join(select * from roles)r on r.id = u.role_id
where u.is_active is True and blocked is False and u.role_id=4
-- limit 100
