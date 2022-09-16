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