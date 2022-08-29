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
    universidades = 'universidad-j.-f.-kennedy'
*/

SELECT	"universidades" AS "university", 
		"carreras" AS "career", 
		TO_DATE("fechas_de_inscripcion",'YY Mon DD' ) AS "inscription_date",    /*  transformamos en tipo de dato de 
                                                                                    formato VARCHART a DATE
                                                                                */ 
		SPLIT_PART("nombres", '-', 1) AS "first_name",                          /*  separamos la columna "nombre" y 
                                                                                    almacenamos el primer campo como 
                                                                                    "first_name"
                                                                                */
		SPLIT_PART("nombres", '-', 2) AS "last_name",                           /*  separamos la columna "nombre" y 
                                                                                    almacenamos el segundo campo como 
                                                                                    "last_name"
                                                                                */
		"sexo" AS "gender",
		AGE(CURRENT_DATE, TO_DATE("fechas_nacimiento",'YY Mon DD')),            /*  calculamos la edad a partir de 
                                                                                    "fechas_nacimiento" y "current_date"
                                                                                
                                                                                */
		"codigos_postales" AS "postal_code", 
		"direcciones" AS "location", 
		"emails" AS "email"
		FROM 	(SELECT *                                                       /*  creamos una subconsulta a paritr de 
                                                                                    la cual obtendremos los registros en 
                                                                                    el rango de fechas establecido
                                                                                    01/9/2020 al 01/02/2021
                                                                                */
				FROM 	"uba_kenedy" 
				WHERE 	TO_DATE("fechas_de_inscripcion",'YY Mon DD' ) 
				BETWEEN TO_DATE('01/09/2020', 'DD/MM/YYYY') 
				AND TO_DATE('01/02/2021', 'DD/MM/YYYY')
				) AS btw_date
		WHERE 	universidades = 'universidad-j.-f.-kennedy';