SELECT
	university, 
	career,
	to_date(inscription_date, 'YYYY/MM/DD') as inscription_date,
	split_part(nombre,' ', 1) as first_name,
	split_part(nombre, ' ', 2) as last_name,
	sexo as gender,
	round((Current_Date - to_date(birth_date, 'YYYY/MM/DD'))/365.25) as age,
	localidad2.codigo_postal as postal_code,
	"location",
	email
FROM jujuy_utn
INNER JOIN localidad2
	ON localidad2.localidad = UPPER(jujuy_utn.location)
WHERE university = 'universidad tecnológica nacional'
	AND to_date(inscription_date, 'YYYY/MM/DD') 
	 BETWEEN'2020/09/01' 
	  AND'2021/02/01';
	  
	  
	  
	  
SELECT
	universidad as university,
	careers as career,
	to_date(fecha_de_inscripcion, 'DD/MON/YY') as inscription_date,
	split_part("names", '_', 1) as first_name,
	split_part("names", '_', 2) as last_name,
	sexo as gender,
	round((CURRENT_DATE - to_date(birth_dates, 'DD/MON/YY'))/365.25) as age,
	pa.codigo_postal as postal_code,
	localidad2.localidad as "location",
	correos_electronicos as email
FROM palermo_tres_de_febrero as pa
INNER JOIN localidad2
	ON localidad2.codigo_postal = cast(pa.codigo_postal as numeric)
WHERE universidad = 'universidad_nacional_de_tres_de_febrero'
	AND to_date(fecha_de_inscripcion, 'DD/MON/YY')
	BETWEEN '2020/09/01'
	 AND '2021/02/01';