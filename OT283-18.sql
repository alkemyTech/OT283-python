SELECT
universidad AS university, carrerra as career,fechaiscripccion as inscription_date,
SPLIT_PART(nombrre,' ',1) AS firstname,
SPLIT_PART(nombrre,' ',2) AS lastname,
sexo AS gender,
DATE_PART('year',current_date)-DATE_PART('year',TO_DATE(nacimiento,'DD/MM/YYYY')) AS age,
codgoposstal AS postal_code,
direccion AS location,
eemail AS email
FROM moron_nacional_pampa
WHERE  universidad = 'Universidad de morón' AND
TO_DATE(fechaiscripccion,'DD/MM/YYYY') BETWEEN '2020-09-01'AND'2021-02-01'







WITH QUERY1 AS 
    (SELECT rio.univiersities AS university,
    rio.carrera AS career,
    inscription_dates AS inscription_date,
    SPLIT_PART(rio.names,'-',1) AS firstname,
    SPLIT_PART(rio.names,'-',2) AS lastname,
    rio.sexo AS gender,
    rio.direcciones AS location,
    loc2.codigo_postal AS postal_code,
    REPLACE(LOWER(rio.localidad),' ','-') AS localidad,
    rio.email,
    id
    FROM public.rio_cuarto_interamericana rio
    INNER JOIN public.localidad2 loc2 on LOWER(loc2.localidad) = REPLACE(LOWER(rio.localidad),' ','-')),
QUERY2 AS (
    WITH q1 as
            (SELECT CASE
            WHEN SPLIT_PART(rio.fechas_nacimiento,'/',1)>='21' THEN CONCAT('19',SPLIT_PART(rio.fechas_nacimiento,'/',1))
            ELSE CONCAT('20',SPLIT_PART(rio.fechas_nacimiento,'/',1))
            END AS year,
            id,
            CONCAT(SPLIT_PART(rio.fechas_nacimiento,'/',2),'-',
                   SPLIT_PART(rio.fechas_nacimiento,'/',3)) AS monthday
            FROM public.rio_cuarto_interamericana rio),
    q2 as(
            SELECT TO_DATE(CONCAT(monthday,'-',year),'Mon-DD-YYYY') as dateborn,
            id
            FROM q1)
        SELECT DATE_PART('year',current_date)-DATE_PART('year',dateborn) AS age,
        id
        from q2)
SELECT QUERY1.university,QUERY1.career,QUERY1.inscription_date,QUERY1.firstname,
QUERY1.lastname,QUERY1.gender,QUERY2.age,QUERY1.location,QUERY1.postal_code,QUERY1.email
FROM QUERY1
INNER JOIN QUERY2 ON QUERY2.id =QUERY1.id
WHERE  QUERY1.university='Universidad-nacional-de-río-cuarto' AND
TO_DATE(QUERY1.inscription_date,'DD/Mon/YY') BETWEEN '2020-09-01'AND'2021-02-01'