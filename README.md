# Scala Spark y Spark SQL (Laboratorio #4)

Laboratorio #4 del curso de `Análisis en Macrodatos`. El dataset utilizado fue descargado de [Kaggle](https://www.kaggle.com/datasets/itachi9604/disease-symptom-description-dataset).

## Requisitos

Tener [docker](https://docs.docker.com/desktop/install/linux-install/) y [docker-compose](https://docs.docker.com/compose/install/linux/) instalados en su sistema.

## Instalación

```sh
docker-compose up -d --build
```

## Ejecución

```sh
# Ingresar al contenedor que tiene spark
docker exec -it spark-shell bash

# Ejecutar cualquier archivo .scala
../bin/spark-shell -i <filename>.scala
```