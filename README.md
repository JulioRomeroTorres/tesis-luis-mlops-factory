# Estructura lógica del repositorio

```text
📦 Proyecto
││
├── 📁 .github/workflows/
│   └── 🐙 deploy.yml # Archivo de configuración para pipelines en Github 

├── ☁️ cloudbuild
│   ├─ 🔄 pipelines.yaml # Archivo para desplegar infraestructura de los pipelines
│   ├─ 🔌 api.yaml # Archivo para desplegar infraestructura del API 
│   └─ 🖥️ web-ui.yaml # Archivo para desplegar infraestructura de la web
│
├── 🔄 pipelines/ # Pipelines de entrenamiento e inferencia
│   ├── 📄 (...) Implementación en Código
│   └── 🐳 Dockerfile #Archivo para empaquetar lo implementado
│
├── 🔌 inference/ # Pipelines de entrenamiento e inferencia
│   ├── 📄 (...) Implementación en Código
│   └── 🐳 Dockerfile #Archivo para empaquetar lo implementado
│
├── 🖥️ web-ui/
    ├── 📄 (...) Implementación en Código
    └── 🐳 Dockerfile #Archivo para empaquetar lo implementado