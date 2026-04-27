import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { jazzSvelteKit } from "jazz-tools/dev/sveltekit";

export default defineConfig({
  plugins: [jazzSvelteKit(), svelte()],
});
