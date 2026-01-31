import { defineConfig } from "tsdown"

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  outDir: "dist",
  sourcemap: false,
  dts: true,
  shims: false,
  clean: true,
  treeshake: true,
  target: false,
})
