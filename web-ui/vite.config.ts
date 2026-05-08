import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vitejs.dev/config/
export default defineConfig(() => {
  const apiPort = process.env.API_PORT || 8000;
  const frontendPort = process.env.FRONTEND_PORT ? parseInt(process.env.FRONTEND_PORT) : 5173;
  const catalogBaseUrl = process.env.VITE_CATALOG_API_BASE_URL;

  // Always add the telemetry-dashboard proxy.
  // VITE_TELEMETRY_DASHBOARD_API_BASE_URL controls direct access in prod;
  // this proxy is the local-dev fallback when that var is unset.
  const proxy: Record<string, string | import('vite').ProxyOptions> = {
    '/telemetry-dashboard-api': {
      target: 'http://localhost:8000',
      changeOrigin: true,
      rewrite: (path) => path.replace(/^\/telemetry-dashboard-api/, ''),
    },
  };

  // Only enable the /api proxy when no external catalog URL is configured.
  if (!catalogBaseUrl) {
    proxy['/api'] = {
      target: `http://localhost:${apiPort}`,
      changeOrigin: true,
    };
  }

  return {
    plugins: [react(), tailwindcss()],
    server: {
      port: frontendPort,
      proxy,
    }
  }
})
