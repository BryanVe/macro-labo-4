# Establecer la imagen base
FROM postgres:alpine

# Copiar scripts SQL a la imagen
COPY *.sql /docker-entrypoint-initdb.d/
