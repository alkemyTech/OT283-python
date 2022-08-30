SELECT
	university as university, 
	career as career,
	to_date(inscription_date, 'YYYY/MM/DD') as inscription_date,
	split_part(nombre,' ', 1) as first_name,
	split_part(nombre, ' ', 2) as last_name,
	sexo as gender,
	round((Current_Date - to_date(birth_date, 'YYYY/MM/DD'))/365.25) as age,
	localidad2.codigo_postal as postal_code,
	"location",
	email as email
FROM jujuy_utn as utn
INNER JOIN localidad2
	ON localidad2.localidad = UPPER(utn.location)
WHERE university = 'universidad tecnológica nacional'
	AND to_date(inscription_date, 'YYYY/MM/DD') 
	 BETWEEN'2020/09/01' 
	  AND'2021/02/01';