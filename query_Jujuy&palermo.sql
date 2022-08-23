-- Universidad Nacional de JuJuy
select
	age(birth_date::date) as age,
	split_part(nombre, ' ', 1) as first_name,
	split_part(nombre, ' ', 2) as last_name,
	x.arr[array_upper(arr,1)] as postal_code,
	university,
	career,
	inscription_date,
	sexo as gender,
	location,
	email
from jujuy_utn j
	cross join lateral (values (string_to_array(j.direccion, ' '))) x(arr) 
where to_date(inscription_date, 'yyyy/mm/dd') 
	between to_date('01/09/2020', 'dd/mm/yyyy') 
		and to_date('01/02/2021', 'dd/mm/yyyy') 
;
-- Universidad de Palermo
select
	to_date(replace(birth_dates,
					substring(birth_dates from 7 for 8),
					'/19' || substring(birth_dates from 8 for 8)),
			'DD/Mon/YYYY') as age,
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
