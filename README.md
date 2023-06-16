# Scala Spark y Spark SQL (Laboratorio #4)

Laboratorio #4 del curso de `Análisis en Macrodatos`

## Requisitos

Tener [`docker`](https://docs.docker.com/desktop/install/linux-install/) y [`docker-compose`](https://docs.docker.com/compose/install/linux/) instalados en su sistema.

## Instalación

```sh
bash run.sh
```

## Ejecución

```sh
# Ingresar al contenedor que tiene spark
docker exec -it spark-shell bash

# Ejecutar cualquier archivo .scala
../bin/spark-shell -i <filename>.scala
```