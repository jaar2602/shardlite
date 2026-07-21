/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["'IBM Plex Sans'", "system-ui", "sans-serif"],
        mono: ["'IBM Plex Mono'", "ui-monospace", "monospace"],
      },
      colors: {
        // Carbon Gray 100 (dark) + Gray 10 (light) token subset, plus the signature blue-60.
        carbon: {
          bg: "#161616",
          layer: "#262626",
          layer2: "#393939",
          border: "#393939",
          field: "#262626",
          text: "#f4f4f4",
          "text-2": "#c6c6c6",
          "text-3": "#8d8d8d",
          blue: "#0f62fe",
          "blue-hover": "#0353e9",
          green: "#42be65",
          red: "#fa4d56",
          yellow: "#f1c21b",
        },
      },
    },
  },
  plugins: [],
};
