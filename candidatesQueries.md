## Consulta 1:

Obtén todas las enfermedades junto con sus síntomas.

```SQL
SELECT d.disease, s.*
FROM disease_descriptions d
INNER JOIN disease_symptoms s ON d.disease = s.disease;
```

## Consulta 2:

Obtén las enfermedades y sus precauciones correspondientes para aquellas enfermedades que tengan una gravedad de síntoma mayor a 5.

```SQL
SELECT p.*, d.disease
FROM disease_precautions p
INNER JOIN symptom_severities s ON p.disease = s.symptom
WHERE s.weight > 5;
```

## Consulta 3:

Obtén todas las enfermedades que tienen el síntoma 'Fiebre'.
```SQL
SELECT d.disease
FROM disease_descriptions d
INNER JOIN disease_symptoms s ON d.disease = s.disease
WHERE s.symptom1 = 'Fiebre';
```

## Consulta 4:

Obtén la descripción y las precauciones para la enfermedad 'Malaria'.
```SQL
SELECT d.description, p.*
FROM disease_descriptions d
INNER JOIN disease_precautions p ON d.disease = p.disease
WHERE d.disease = 'Malaria';
```

## Consulta 5:

Obtén todas las enfermedades que tienen síntomas con una gravedad mayor a 3.
```SQL
SELECT d.disease
FROM disease_descriptions d
INNER JOIN disease_symptoms s ON d.disease = s.disease
INNER JOIN symptom_severities ss ON s.symptom1 = ss.symptom
WHERE ss.weight > 3;
```

## Consulta 6:

Obtén las precauciones relacionadas con los síntomas con una gravedad menor o igual a 2.
```SQL
SELECT p.*
FROM disease_precautions p
INNER JOIN symptom_severities s ON p.disease = s.symptom
WHERE s.weight <= 2;
```

## Consulta 7:

Obtén las enfermedades y sus síntomas correspondientes para aquellas enfermedades que tienen una descripción que contiene la palabra 'respiratorio'.
```SQL
SELECT d.disease, s.*
FROM disease_descriptions d
INNER JOIN disease_symptoms s ON d.disease = s.disease
WHERE d.description LIKE '%respiratorio%';
```

## Consulta 8:

Obtén las enfermedades y sus precauciones correspondientes para aquellas enfermedades que tienen el síntoma 'Dolor de cabeza'.

```SQL
SELECT p.*, d.disease
FROM disease_precautions p
INNER JOIN disease_symptoms s ON p.disease = s.disease
INNER JOIN disease_descriptions d ON d.disease = s.disease
WHERE s.symptom2 = 'Dolor de cabeza';
```

## Consulta 9:

Obtén todas las enfermedades que tienen síntomas con una gravedad mayor a 4 y precauciones relacionadas con esos síntomas.

```SQL
SELECT d.disease, s.*, p.*
FROM disease_descriptions d
INNER JOIN disease_symptoms s ON d.disease = s.disease
INNER JOIN symptom_severities ss ON s.symptom1 = ss.symptom
INNER JOIN disease_precautions p ON p.disease = d.disease
WHERE ss.weight > 4;
```

## Consulta 10:

Obtén las enfermedades que tienen síntomas con una gravedad mayor a 3 y que también tienen precauciones relacionadas con esos síntomas.

```SQL
SELECT d.disease
FROM disease_descriptions d
INNER JOIN disease_symptoms s ON d.disease = s.disease
INNER JOIN symptom_severities ss ON s.symptom1 = ss.symptom
INNER JOIN disease_precautions p ON p.disease = d.disease
WHERE ss.weight > 3;
```

## Consulta 11:

Obtén las enfermedades que tienen al menos un síntoma con gravedad mayor a 5 y que también tienen precauciones relacionadas con esos síntomas.

```SQL
SELECT d.disease
FROM disease_descriptions d
INNER JOIN disease_symptoms s ON d.disease = s.disease
INNER JOIN symptom_severities ss ON (s.symptom1 = ss.symptom OR s.symptom2 = ss.symptom OR s.symptom3 = ss.symptom OR s.symptom4 = ss.symptom)
INNER JOIN disease_precautions p ON p.disease = d.disease
WHERE ss.weight > 5;
```

## Consulta 12:

Obtén todas las enfermedades que tienen al menos un síntoma con gravedad mayor a 3 y ordena los resultados por la gravedad del primer síntoma en orden descendente.

```SQL
SELECT d.disease, s.*
FROM disease_descriptions d
INNER JOIN disease_symptoms s ON d.disease = s.disease
INNER JOIN symptom_severities ss ON s.symptom1 = ss.symptom
WHERE ss.weight > 3
ORDER BY ss.weight DESC;
```

## Consulta 13:

Obtén las precauciones relacionadas con los síntomas de la enfermedad 'Gripe' ordenadas alfabéticamente.

```SQL
SELECT p.*
FROM disease_precautions p
INNER JOIN disease_symptoms s ON p.disease = s.disease
WHERE p.disease = 'Gripe'
ORDER BY p.precaution1;
```

## Consulta 14:

Obtener la descripción de una enfermedad junto con las precauciones relacionadas.
   
```sql
SELECT dd.disease, dd.description, dp.precaution1, dp.precaution2, dp.precaution3, dp.precaution4
FROM disease_descriptions dd
JOIN disease_precautions dp ON dd.disease = dp.disease
WHERE dd.disease = 'nombre_enfermedad';
```   

## Consulta 15:

Obtener todas las enfermedades que tienen un síntoma específico.
   
```SQL
SELECT ds.disease, ds.symptom1
FROM disease_symptoms ds
JOIN disease_descriptions dd ON ds.disease = dd.disease
WHERE ds.symptom1 = 'nombre_sintoma';
```   

## Consulta 16:

Obtener todas las enfermedades junto con sus descripciones y los síntomas asociados.
   
```SQL
SELECT dd.disease, dd.description, ds.symptom1, ds.symptom2, ds.symptom3, ds.symptom4, ds.symptom5, ds.symptom6, ds.symptom7, ds.symptom8, ds.symptom9, ds.symptom10, ds.symptom11, ds.symptom12, ds.symptom13, ds.symptom14, ds.symptom15, ds.symptom16, ds.symptom17
FROM disease_descriptions dd
JOIN disease_symptoms ds ON dd.disease = ds.disease;
```   

## Consulta 17:

Obtener los síntomas de una enfermedad específica junto con sus respectivas severidades.
   
```SQL
SELECT ds.symptom1, ds1.weight
FROM disease_symptoms ds
JOIN symptom_severities ss ON ds.symptom1 = ss.symptom
WHERE ds.disease = 'nombre_enfermedad';
```   

## Consulta 18:

Obtener todas las enfermedades que tienen precauciones relacionadas con un síntoma específico.
   
```SQL
SELECT dd.disease, dp.precaution1, dp.precaution2, dp.precaution3, dp.precaution4
FROM disease_descriptions dd
JOIN disease_precautions dp ON dd.disease = dp.disease
JOIN disease_symptoms ds ON dd.disease = ds.disease
WHERE ds.symptom1 = 'nombre_sintoma';
```   

## Consulta 19:

Obtener las enfermedades junto con las precauciones y severidades relacionadas con un síntoma específico.
   
```SQL
SELECT dd.disease, dp.precaution1, dp.precaution2, dp.precaution3, dp.precaution4, ss.weight
FROM disease_descriptions dd
JOIN disease_precautions dp ON dd.disease = dp.disease
JOIN disease_symptoms ds ON dd.disease = ds.disease
JOIN symptom_severities ss ON ds.symptom1 = ss.symptom
WHERE ds.symptom1 = 'nombre_sintoma';
```   

## Consulta 20:

Obtener todas las enfermedades junto con sus descripciones y el peso total de las severidades de los síntomas.
   
```SQL
SELECT dd.disease, dd.description, SUM(ss.weight) AS total_severity
FROM disease_descriptions dd
JOIN disease_symptoms ds ON dd.disease = ds.disease
JOIN symptom_severities ss ON ds.symptom1 = ss.symptom
GROUP BY dd.disease, dd.description;
```   

## Consulta 21:

Obtener los síntomas con su respectiva severidad para una enfermedad específica.
   
```SQL
SELECT ds.symptom1, ss.weight
FROM disease_symptoms ds
JOIN symptom_severities ss ON ds.symptom1 = ss.symptom
WHERE ds.disease = 'nombre_enfermedad';
```   

## Consulta 22:

Obtener todas las precauciones relacionadas con una enfermedad específica.
   
```SQL
SELECT dp.precaution1, dp.precaution2, dp.precaution3, dp.precaution4
FROM disease_precautions dp
JOIN disease_descriptions dd ON dp.disease = dd.disease
WHERE dd.disease = 'nombre_enfermedad';
```   

## Consulta 23:

Obtener todas las enfermedades junto con el número total de síntomas asociados.
    
```SQL
SELECT dd.disease, COUNT(*) AS total_symptoms
FROM disease_descriptions dd
JOIN disease_symptoms ds ON dd.disease = ds.disease
GROUP BY dd.disease;
```
