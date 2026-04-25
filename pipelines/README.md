# ml-pm25

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Resumen

Este proyecto sirve para el análisis y detección de posibles fraudes de los sustentos proporcionados por AUNA.

## Stack Tecnológico

- Python 3.10
- Kedro

## Dependencias
- APi Core
- Api Orquestador
- Kedro Pipeline

## Diagrama de Arquitectura



## API Core

## Api Orquestador

## Kedro Pipeline

## Instalación de dependencias

Dado que el principal lenguaje de programación es python, se recomienda crear un entorno virtual usando pyenv o conda, de la siguiente manera:

```
conda create python=3.10 -y -n MY_ENV
```

Luego de ello se procede a instalar las dependencias de la siguiente manera:

```
pip install -r requirements.txt
```

## Integración de un nuevo documento

Para la integración de un nuevo documento a analizar se debe de seguir el siguiente flujo:

![Alt text](integracion_nuevo_documento.png)

Para ello los AI Engineers encargados del proyecto deben de entregar los siguientes input:
- Actualización de la prompt del agente etiquetador.
- Prompt del nuevo agente especializado en la actualización del nuevo agente.
- Clase de Pydantic o estructura en JSON de la respuesta estructurada.

Seguido de ello se puede usar algun LLM que brinda la query necesario para la creación de Bigquery
en donde se almacene la data, así como el esquema para la carga.

## Testing Local del Pipeline
Como se mencionó lineas arriba, a nivel del proyecto de kedro se cuenta con 2 pipeline, en caso de que se quiero probar el pipeline que ejecute la extracción de entidades se debe de usar el siguiente comando

kedro run -p batch_process


kedro run -p batch_process --parallel --workers=4

kedro run --runner=ParallelRunner