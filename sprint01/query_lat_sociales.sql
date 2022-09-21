/*
Seleccionamos los siguientes campos: 
    universidades (university), 
    carreras (career), 
    fechas_de_inscripcion (inscription_date), 
    nombres (first_name), 
    nombres (last_name), 
    sexo (gender), 
    edad (fecha actual - fecha de nacimiento) (age), 
    codigos_postales (postal_code), 
    direcciones (location), 
    emails (email),
    de aquellos registros donde se cumpla la condición
    universidades = 'universidad-de-buenos-aires'.
*/

SELECT	"universities" AS "university", 
		"careers" AS "career", 
		TO_DATE("inscription_dates",'DD-MM-YYYY' ) AS "inscription_date",    /*  transformamos en tipo de dato de 
                                                                                    formato VARCHART a DATE
                                                                                */ 
		SPLIT_PART("names", '-', 1) AS "first_name",                          /*  separamos la columna "nombre" y 
                                                                                    almacenamos el primer campo como 
                                                                                    "first_name"
                                                                                */
		SPLIT_PART("names", '-', 2) AS "last_name",                           /*  separamos la columna "nombre" y 
                                                                                    almacenamos el segundo campo como 
                                                                                    "last_name"
                                                                                */
		"sexo" AS "gender",
		AGE(CURRENT_DATE, TO_DATE("birth_dates",'DD-MM-YYYY')),            /*  calculamos la edad a partir de 
                                                                                    "fechas_nacimiento" y "current_date"
                                                                                
                                                                                */
		"locations" AS "location", 
		"direccion" AS "postal_code", 
		"emails" AS "email"
		FROM 	(SELECT *                                                       /*  creamos una subconsulta a paritr de 
                                                                                    la cual obtendremos los registros en 
                                                                                    el rango de fechas establecido
                                                                                    01/9/2020 al 01/02/2021
                                                                                */
				FROM 	lat_sociales_cine
				WHERE 	TO_DATE("inscription_dates",'DD-MM-YYYY' ) 
				BETWEEN TO_DATE('01/09/2020', 'DD/MM/YYYY') 
				AND TO_DATE('01/02/2021', 'DD/MM/YYYY')
				) AS btw_date
		WHERE 	universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES';