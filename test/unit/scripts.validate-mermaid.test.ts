// test/unit/scripts.validate-mermaid.test.ts
import { describe, expect, test } from "bun:test";
import { extractMermaidBlocks } from "../../scripts/validate-mermaid.ts";

describe("extractMermaidBlocks", () => {
  test("returns empty array when no mermaid fences present", () => {
    const md = "# Heading\n\nSome prose.\n\n```ts\nconst x = 1;\n```\n";
    expect(extractMermaidBlocks(md)).toEqual([]);
  });

  test("extracts a single mermaid block", () => {
    const md = [
      "# Title",
      "",
      "```mermaid",
      "flowchart LR",
      "  A --> B",
      "```",
      "",
      "trailing prose",
      "",
    ].join("\n");
    expect(extractMermaidBlocks(md)).toEqual(["flowchart LR\n  A --> B"]);
  });

  test("extracts multiple mermaid blocks in order", () => {
    const md = [
      "```mermaid",
      "graph TD",
      "  A --> B",
      "```",
      "",
      "prose between",
      "",
      "```mermaid",
      "sequenceDiagram",
      "  A->>B: hi",
      "```",
      "",
      "more",
      "",
      "```mermaid",
      "flowchart LR",
      "  X --> Y",
      "```",
    ].join("\n");
    const blocks = extractMermaidBlocks(md);
    expect(blocks).toHaveLength(3);
    expect(blocks[0]).toBe("graph TD\n  A --> B");
    expect(blocks[1]).toBe("sequenceDiagram\n  A->>B: hi");
    expect(blocks[2]).toBe("flowchart LR\n  X --> Y");
  });

  test("ignores non-mermaid code fences interleaved with mermaid ones", () => {
    const md = [
      "```ts",
      "const x = 1;",
      "```",
      "",
      "```mermaid",
      "graph TD",
      "  A --> B",
      "```",
      "",
      "```bash",
      "echo hi",
      "```",
    ].join("\n");
    const blocks = extractMermaidBlocks(md);
    expect(blocks).toEqual(["graph TD\n  A --> B"]);
  });

  test("tolerates trailing whitespace on the opening fence", () => {
    const md = "```mermaid   \nflowchart LR\n  A --> B\n```";
    expect(extractMermaidBlocks(md)).toEqual(["flowchart LR\n  A --> B"]);
  });
});
