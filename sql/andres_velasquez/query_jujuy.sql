-- Universidad Nacional de JuJuy get data
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
where to_date(inscription_date, 'yyyy-mm-dd') 
	between to_date('01-09-2020', 'dd-mm-yyyy') 
		and to_date('01-02-2021', 'dd-mm-yyyy') 
;

