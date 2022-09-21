-- Universidad de Palermo get data
select
	CASE	
		WHEN birth_dates LIKE '%%/00'
			THEN age(to_date(replace(birth_dates,
					substring(birth_dates from 7 for 8),
					'/20' || substring(birth_dates from 8 for 8)),
				'DD/Mon/YYYY'))
			ELSE age(to_date(replace(birth_dates,
								substring(birth_dates from 7 for 8),
								'/19' || substring(birth_dates from 8 for 8)),
						'DD/Mon/YYYY')) 
	END as age,
	split_part(names, '_', 1) as first_name,
	split_part(names, '_', 2) as last_name,
	codigo_postal as postal_code,
	universidad as university,
	careers as career,
	fecha_de_inscripcion::date as inscription_date,
	sexo as gender,
	null as location,
	null as email
from palermo_tres_de_febrero ptdf 
where fecha_de_inscripcion::date 
	between '01/09/2020'::date 
		and	'01/02/2021'::date
;