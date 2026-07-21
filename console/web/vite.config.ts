import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base '/' so asset URLs are absolute and resolve from any client-side route (e.g.
// /connections/local), not relative to the current path. In dev, proxy /api to the console
// backend so the SPA and its API share an origin exactly as they do in production.
export default defineConfig({
  plugins: [react()],
  base: "/",
  build: { outDir: "dist", emptyOutDir: true },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:7100",
    },
  },
});
