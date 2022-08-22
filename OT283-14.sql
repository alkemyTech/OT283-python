SELECT 
   universidad AS university, 
   carrera AS career, 
   fecha_de_inscripcion AS inscription_date, 
   SPLIT_PART(name, ' ', 1) AS first_name,
   SPLIT_PART(name, ' ', 2) AS last_name,
   sexo AS gender,
   AGE(TO_DATE(fecha_nacimiento, 'YYYY-MM-DD')) AS age,
   codigo_postal AS postal_code,
   direccion AS location,
   correo_electronico AS email 
FROM public.flores_comahue
WHERE 
   universidad = 'UNIV. NACIONAL DEL COMAHUE' 
   AND TO_DATE(fecha_de_inscripcion, 'YYYY-MM-DD') BETWEEN '2020-09-01' AND '2021-02-01'
ORDER BY inscription_date;


SELECT 
   s.universidad AS university,
   s.carrera AS career, 
   TO_DATE(s.fecha_de_inscripcion, 'DD-Mon-YY') AS inscription_date, 
   SPLIT_PART(s.nombre, '_', 1) AS first_name,
   SPLIT_PART(s.nombre, '_', 2) AS last_name, 
   s.sexo AS gender,
   AGE(TO_DATE(s.fecha_nacimiento, 'DD-Mon-YY')) AS age,
   l2.codigo_postal AS postal_code,
   s.direccion AS location,
   s.email 
FROM public.salvador_villa_maria s 
INNER JOIN public.localidad2 l2 ON l2.localidad = REPLACE(s.localidad, '_', ' ')
WHERE 
   universidad = 'UNIVERSIDAD_DEL_SALVADOR' 
   AND (to_date(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '2020-09-01' AND '2021-02-01')
ORDER BY inscription_date;

