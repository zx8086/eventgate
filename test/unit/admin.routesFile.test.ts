// test/unit/admin.routesFile.test.ts
import { afterEach, describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readRoutesFile, writeRoutesFile } from "../../src/admin/routesFile.ts";

let dir: string;

afterEach(() => {
  if (dir) rmSync(dir, { recursive: true, force: true });
});

function mkdir(): string {
  dir = mkdtempSync(join(tmpdir(), "eventgate-routes-"));
  return dir;
}

describe("readRoutesFile", () => {
  it("returns the parsed array", async () => {
    const d = mkdir();
    const path = join(d, "routes.json");
    writeFileSync(path, JSON.stringify([{ name: "a" }]));
    const result = await readRoutesFile(path);
    expect(result).toEqual([{ name: "a" }]);
  });

  it("throws when the file does not exist", async () => {
    const d = mkdir();
    const path = join(d, "missing.json");
    await expect(readRoutesFile(path)).rejects.toThrow();
  });

  it("throws when the file is not valid JSON", async () => {
    const d = mkdir();
    const path = join(d, "bad.json");
    writeFileSync(path, "not json");
    await expect(readRoutesFile(path)).rejects.toThrow();
  });

  it("throws when the parsed value is not an array", async () => {
    const d = mkdir();
    const path = join(d, "obj.json");
    writeFileSync(path, JSON.stringify({ foo: "bar" }));
    await expect(readRoutesFile(path)).rejects.toThrow(/expected JSON array/i);
  });
});

describe("writeRoutesFile", () => {
  it("writes the routes atomically (no .tmp left behind on success)", async () => {
    const d = mkdir();
    const path = join(d, "routes.json");
    await writeRoutesFile(path, [{ name: "a", path: "/x" }]);
    const round = await readRoutesFile(path);
    expect(round).toEqual([{ name: "a", path: "/x" }]);
    expect(existsSync(`${path}.tmp`)).toBe(false);
  });

  it("overwrites an existing file", async () => {
    const d = mkdir();
    const path = join(d, "routes.json");
    await writeRoutesFile(path, [{ name: "old" }]);
    await writeRoutesFile(path, [{ name: "new" }]);
    expect(await readRoutesFile(path)).toEqual([{ name: "new" }]);
  });
});
