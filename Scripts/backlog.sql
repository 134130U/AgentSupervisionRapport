select u.id as agent_id,  concat(u.first_name, ' ',u.last_name) agent,
                            t.type_ticket, count(t.id) as backlog
                        from tickets t
                            left join users u on t.assigned_to = u.id
                        where status = 'à faire' and assigned_at is not null
                        and t.type_ticket in ('Ticket technique', 'Reposséder')
                        and t.assigned_to is not null and u.role_id in (11,18,5,14)
group by 1,2,3