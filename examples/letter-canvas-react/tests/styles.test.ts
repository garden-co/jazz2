import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const stylesPath = fileURLToPath(new URL("../src/styles.css", import.meta.url));
const styles = readFileSync(stylesPath, "utf8");

describe("letter canvas typography", () => {
  it("uses only the boldest docs display font file for drawer and canvas letters", () => {
    expect(styles).toContain('font-family: "main_font";');
    expect(styles).toContain(
      'src: url("../../../docs/public/fonts/main-font-latin-700-normal.woff2")',
    );
    expect(styles).not.toContain("main-font-latin-400-normal.woff2");
    expect(styles).not.toContain("main-font-latin-500-normal.woff2");
    expect(styles).toMatch(/\.canvas-letter\s*\{[\s\S]*font-family:[\s\S]*main_font/);
    expect(styles).toMatch(/\.canvas-letter-preview\s*\{[\s\S]*font-family:[\s\S]*main_font/);
    expect(styles).toMatch(/\.drawer-tile\s*\{[\s\S]*font-family:[\s\S]*main_font/);
  });

  it("hides the location QR overlay on mobile viewports", () => {
    expect(styles).toMatch(
      /@media\s*\(max-width:\s*767px\)\s*\{[\s\S]*\.location-qr\s*\{[\s\S]*display:\s*none;/,
    );
  });
});
