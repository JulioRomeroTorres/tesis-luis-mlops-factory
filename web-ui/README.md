# Agent AI Hub - Frontend

Repositorio central del **Frontend** para el proyecto *Agent AI Hub*. Esta aplicación funciona como plataforma principal para el manejo, catalogamiento, prueba interactiva y registro de agentes de Inteligencia Artificial.

## Tecnologías Principales
- **React 19 & TypeScript**: Librería principal de vista con tipado estricto.
- **Vite**: Bundler rápido para entornos locales y empaquetado de producción.
- **Tailwind CSS v4 & Lucide React**: Para estilización rápida mediante clases utilitarias y set de íconos robusto.
- **Xyflow (React Flow) & Monaco Editor**: Bibliotecas para control gráfico de workflows conversacionales generativos y edición de código embebida.

## Documentación del Proyecto

Revisa la carpeta `/docs` para entender a detalle la organización interna, el diseño de integración de API, procesos de despliegue y herramientas:

- 🏗️ **[Arquitectura (`docs/ARCHITECTURE.md`)](./docs/ARCHITECTURE.md)**: Explicación de la estructura de módulos (`Catalog`, `Playground`, `Registry`), sistema de rutas y enrutamiento lateral.
- 📦 **[Dependencias (`docs/DEPENDENCIES.md`)](./docs/DEPENDENCIES.md)**: Motivos detrás de la elección del Stack y principales librerías utilizadas.
- 🚀 **[Despliegue (`docs/DEPLOYMENT.md`)](./docs/DEPLOYMENT.md)**: Estrategia de publicación en **Azure App Service** vía pipeline estático montado usando *Flask catch-all*.
- 🔌 **[Integración API (`docs/API_INTEGRATION.md`)](./docs/API_INTEGRATION.md)**: Reglas, URL Bases y diseño de la pasarela entre este Frontends y los distintos dominios del ecosistema AI.

> Si vas a ejecutar el proyecto de forma local, en vez de configurar App Service asegúrate de preparar tu archivo `.env` tomando como base el `.env.example`.

## Flujo CI/CD con GitHub Actions

El repositorio cuenta con automatización CI/CD configurada usando GitHub Actions (`.github/workflows/deploy.yml`). 
Al hacer `push` o fusionar código hacia la rama `main`, la Action realiza automáticamente:

1. El proceso `npm run build` inyectando como *Variables* de GitHub los valores expuestos (URLs del Catalog base, Playground, Default user config, etc).
2. Empaquetado estático con un servidor Python embebido.
3. Login automático por CLI a Azure mediante los secretos del Service Principal (`APP_AZURE_CLIENT_ID`, `APP_AZURE_CLIENT_SECRET`, `APP_AZURE_TENANT_ID`).
4. Escritura de *AppSettings* y despliegue **asíncrono** (para obviar timeouts de AppService) al recurso: `wapps01` en `rg-agents-lab01`.
