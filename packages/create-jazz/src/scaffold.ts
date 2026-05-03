import * as fs from "node:fs";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import {
  resolveLocalDeps,
  resolveRemoteDeps,
  type PackageManifest,
  type ResolveProgressCallback,
} from "./deps.js";

const REPO = "garden-co/jazz";
const BRANCH = "main";
const DEFAULT_STARTER = "next-betterauth";

export const KNOWN_STARTERS = [
  "next-betterauth",
  "next-localfirst",
  "next-hybrid",
  "sveltekit-betterauth",
  "sveltekit-localfirst",
  "sveltekit-hybrid",
] as const;
export type StarterName = (typeof KNOWN_STARTERS)[number];

function isKnownStarter(name: string): name is StarterName {
  return (KNOWN_STARTERS as readonly string[]).includes(name);
}

/**
 * npm package-name rules: lowercase, URL-safe, no whitespace, no leading dot
 * or underscore, no slashes except in a single leading scope. We apply them
 * to the scaffolded project's directory name too, since it becomes the
 * package.json `name` field downstream.
 */
const APP_NAME_RE = /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]{0,213}$/;

export function validateAppName(name: string): void {
  if (!APP_NAME_RE.test(name)) {
    throw new Error(
      `Invalid app name "${name}". Use lowercase letters, numbers, hyphens, dots, or underscores; no spaces, slashes, or leading dots.`,
    );
  }
}

export interface ScaffoldOptions {
  appName: string;
  targetDir: string;
  pm: string | null;
  starter?: string;
  git?: boolean;
  onStep?: (label: string) => void;
  /**
   * Runs after git init and before `pnpm install`. Use for anything that must
   * land in the scaffolded project before the install runs — e.g. cloud
   * provisioning that writes `.env`. Receives `onStep` so long-running work
   * (e.g. a provisioning HTTP request) can advance the caller's spinner
   * rather than leaving it stuck on the previous step.
   */
  preInstall?: (ctx: { dir: string; onStep: (label: string) => void }) => Promise<void>;
}

const SCAFFOLD_COPY_SKIP = new Set(["node_modules", ".next", ".jazz", ".turbo", ".env", ".git"]);

async function fetchStarter(starter: StarterName, dir: string): Promise<void> {
  const localPath = process.env.JAZZ_STARTER_PATH;
  if (localPath) {
    await fs.promises.cp(localPath, dir, {
      recursive: true,
      filter: (src) => !SCAFFOLD_COPY_SKIP.has(path.basename(src)),
    });
    return;
  }
  const tiged = (await import("tiged")).default;
  const emitter = tiged(`${REPO}/starters/${starter}#${BRANCH}`, { disableCache: true });
  await emitter.clone(dir);
}

async function resolveManifest(
  manifest: PackageManifest,
  onProgress?: ResolveProgressCallback,
): Promise<PackageManifest> {
  const localPath = process.env.JAZZ_STARTER_PATH;
  if (localPath) {
    return resolveLocalDeps(manifest, path.resolve(localPath, "../.."), onProgress);
  }
  return resolveRemoteDeps(manifest, { repo: REPO, branch: BRANCH }, onProgress);
}

export async function scaffold(options: ScaffoldOptions): Promise<void> {
  validateAppName(options.appName);

  const starter = options.starter ?? DEFAULT_STARTER;
  if (!isKnownStarter(starter)) {
    throw new Error(
      `Unknown starter "${starter}". Available starters: ${KNOWN_STARTERS.join(", ")}.`,
    );
  }

  // Refuse to touch a pre-existing directory — the transactional cleanup
  // below is only safe for a directory we just created ourselves.
  if (fs.existsSync(options.targetDir)) {
    throw new Error(
      `Target directory "${options.targetDir}" already exists. Choose a different name or remove it first.`,
    );
  }
  fs.mkdirSync(options.targetDir, { recursive: true });

  // Fetch → resolve deps → git init are transactional: on failure, remove the
  // directory we just created.
  try {
    options.onStep?.("Fetching starter");
    await fetchStarter(starter, options.targetDir);

    options.onStep?.("Resolving dependencies");
    const pkgJsonPath = path.join(options.targetDir, "package.json");
    const rawManifest = JSON.parse(fs.readFileSync(pkgJsonPath, "utf-8")) as PackageManifest;
    const resolved = await resolveManifest(rawManifest, (done, total) => {
      options.onStep?.(`Resolving dependencies (${done}/${total})`);
    });
    const finalManifest = { ...resolved, name: options.appName };
    fs.writeFileSync(pkgJsonPath, JSON.stringify(finalManifest, null, 2) + "\n", "utf-8");

    // Inherits the user's git identity from `~/.gitconfig`, GIT_AUTHOR_*, etc.
    if (options.git !== false) {
      options.onStep?.("Initialising git");
      runGitInit(options.targetDir);
    }
  } catch (err) {
    fs.rmSync(options.targetDir, { recursive: true, force: true });
    throw err;
  }

  // Pre-install hook runs after the transactional steps; failures from here
  // on leave the project intact so the user can inspect / retry manually.
  if (options.preInstall) {
    await options.preInstall({
      dir: options.targetDir,
      onStep: (label) => options.onStep?.(label),
    });
  }

  // Install is not transactional for the same reason — failure leaves the
  // project intact so the user can retry `npm install` by hand. `execFileSync`
  // with an argv array means no shell interpretation of `pm`.
  if (options.pm) {
    options.onStep?.("Installing dependencies");
    try {
      execFileSync(options.pm, ["install"], { cwd: options.targetDir, stdio: "pipe" });
    } catch (err) {
      const stderr = getStderr(err);
      throw new Error(
        `${options.pm} install failed: ${stderr || (err instanceof Error ? err.message : String(err))}`,
      );
    }
  }
}

function runGitInit(cwd: string): void {
  const execOpts = { cwd, stdio: "pipe" as const };
  try {
    execFileSync("git", ["init"], execOpts);
    execFileSync("git", ["add", "."], execOpts);
    execFileSync("git", ["commit", "-m", "Initial commit"], execOpts);
  } catch (err) {
    const stderr = getStderr(err);
    if (/auto-detect|author identity unknown|please tell me who you are/i.test(stderr)) {
      throw new Error(
        [
          "Git commit failed: no git identity configured.",
          "Set one globally:",
          '  git config --global user.email "you@example.com"',
          '  git config --global user.name  "Your Name"',
          "Or re-run create-jazz with --no-git to skip the initial commit.",
        ].join("\n"),
      );
    }
    throw new Error(`git init failed: ${stderr || String(err)}`);
  }
}

function getStderr(err: unknown): string {
  return err instanceof Error && "stderr" in err
    ? String((err as { stderr: Buffer | string }).stderr)
    : "";
}
