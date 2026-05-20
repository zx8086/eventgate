// scripts/validate-mermaid.ts
import { Glob } from "bun";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const MERMAID_FENCE_RE = /^```mermaid[^\n]*\n([\s\S]*?)\n```/gm;

export function extractMermaidBlocks(markdown: string): string[] {
  const blocks: string[] = [];
  for (const match of markdown.matchAll(MERMAID_FENCE_RE)) {
    blocks.push(match[1]!);
  }
  return blocks;
}

interface BlockFailure {
  file: string;
  index: number;
  stderr: string;
}

async function validateBlock(source: string, workDir: string): Promise<{ ok: true } | { ok: false; stderr: string }> {
  const input = join(workDir, "block.mmd");
  const output = join(workDir, "block.svg");
  writeFileSync(input, source, "utf8");

  // Mermaid CLI runs Puppeteer + headless Chromium. SVG output skips the
  // raster pipeline; parse errors still surface via exit code + stderr.
  const proc = Bun.spawn(
    ["npx", "--yes", "-p", "@mermaid-js/mermaid-cli", "mmdc", "-i", input, "-o", output, "-q"],
    { stdout: "pipe", stderr: "pipe" },
  );
  const [stderrText, exitCode] = await Promise.all([new Response(proc.stderr).text(), proc.exited]);
  if (exitCode === 0) return { ok: true };
  return { ok: false, stderr: stderrText.trim() || `mmdc exited ${exitCode}` };
}

async function discoverMarkdownFiles(repoRoot: string): Promise<string[]> {
  const glob = new Glob("**/*.md");
  const files: string[] = [];
  for await (const rel of glob.scan({ cwd: repoRoot, dot: false })) {
    if (rel.startsWith("node_modules/")) continue;
    if (rel.startsWith(".git/")) continue;
    if (rel.startsWith(".claude/worktrees/")) continue;
    // Fixtures contain intentionally-broken diagrams used by the smoke test.
    if (rel.startsWith("test/fixtures/")) continue;
    files.push(rel);
  }
  files.sort();
  return files;
}

async function main(): Promise<void> {
  const repoRoot = process.cwd();
  const argv = process.argv.slice(2);
  const files = argv.length > 0 ? argv : await discoverMarkdownFiles(repoRoot);

  let totalBlocks = 0;
  const failures: BlockFailure[] = [];
  const workDir = mkdtempSync(join(tmpdir(), "mermaid-validate-"));

  try {
    for (const file of files) {
      const text = await Bun.file(join(repoRoot, file)).text();
      const blocks = extractMermaidBlocks(text);
      if (blocks.length === 0) continue;
      console.log(`[validate-mermaid] ${file}: ${blocks.length} block(s)`);
      for (let i = 0; i < blocks.length; i++) {
        totalBlocks++;
        const result = await validateBlock(blocks[i]!, workDir);
        if (!result.ok) {
          failures.push({ file, index: i + 1, stderr: result.stderr });
        }
      }
    }
  } finally {
    rmSync(workDir, { recursive: true, force: true });
  }

  console.log(`[validate-mermaid] checked ${totalBlocks} block(s) across ${files.length} markdown file(s)`);
  if (failures.length > 0) {
    console.error(`[validate-mermaid] ${failures.length} block(s) failed:`);
    for (const f of failures) {
      console.error(`  - ${f.file} (block #${f.index}):\n${f.stderr.replace(/^/gm, "      ")}`);
    }
    process.exit(1);
  }
}

if (import.meta.main) {
  await main();
}
