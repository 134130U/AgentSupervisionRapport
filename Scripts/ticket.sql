select agent_id,closed_date,agent,type_ticket, to_char(closed_date,'yyyy')::int as annee,
		to_char(closed_date,'mm')::int mois,to_char(closed_date,'dd')::int jour,status,ticket
		from
		(select u.id agent_id, case when status = 'cloturer' and t.closed_at is not null then  t.closed_at::date
							when status = 'cloturer' and t.closed_at isnull then  t.solved_at::date
							when status = 'customer_paid' and t.paid_at is not null then t.paid_at::date
							when status = 'customer_paid' and t.paid_at isnull then t.solved_at::date
							when status = 'à faire'  then t.created_at::date end as closed_date, 
		concat(u.first_name, ' ',u.last_name) agent,t.type_ticket, t.status,count(t.id) as ticket
    from tickets t left join users u on t.assigned_to = u.id
    where t.solved_at is not null and assigned_at is not null
    and t.type_ticket in ('Ticket technique', 'Reposséder')
    and t.assigned_to is not null and u.role_id in (11,18,5,14)
	and status in ('à faire','cloturer','customer_paid') and deleted is False
		 group by 1,2,3,4,5)tab

-- limit 100