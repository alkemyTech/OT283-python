SELECT 
	SUBSTRING(name, 1, position(' ' in name)-1) AS fist_name, 
	SUBSTRING(name, position(' ' in name)+1, char_length(name)) AS last_name, 
	sexo AS gender, 
	direccion AS location, 
	correo_electronico AS Email, 
	round((CURRENT_DATE - fecha_nacimiento::date)/365.25) AS age, 
	universidad AS university, 
	fecha_de_inscripcion::date AS inscription_date, 
	carrera AS career, 
	codigo_postal AS postal_code 
FROM flores_comahue 
WHERE universidad = 'UNIVERSIDAD DE FLORES' 
	AND fecha_de_inscripcion::date >= '2020-09-01' 
	AND fecha_de_inscripcion::date <= '2021-02-01'

SELECT 
	SUBSTRING(nombre, 1, position('_' in nombre)-1) AS fist_name, 
	SUBSTRING(nombre, position('_' in nombre)+1, 
	char_length(nombre)) AS last_name, sexo AS gender, 
	direccion AS location, 
	email AS Email, 
	round((CURRENT_DATE - fecha_nacimiento::date)/365.25) AS age, 
	universidad AS university, 
	fecha_de_inscripcion::date AS inscription_date, 
	carrera AS career,
	localidad2.codigo_postal as postal_code
FROM salvador_villa_maria
INNER JOIN localidad2
	on localidad2.localidad = salvador_villa_maria.localidad 
WHERE universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA' 
	AND fecha_de_inscripcion::date >= '2020-09-01' 
	AND fecha_de_inscripcion::date <= '2021-02-01'