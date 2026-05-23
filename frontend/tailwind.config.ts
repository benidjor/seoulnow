import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        congest: {
          relaxed: "#28a745",
          normal: "#ffc107",
          busy: "#fd7e14",
          crowded: "#dc3545",
          unknown: "#9ca3af",
        },
      },
    },
  },
  plugins: [],
};

export default config;
