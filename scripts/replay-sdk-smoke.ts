// scripts/replay-sdk-smoke.ts
// Verifies header Map<Buffer,Buffer> lookup behavior for the replay subsystem.
// Result drives whether readHeader() in src/replay/headers.ts can use direct
// Map.get(Buffer.from(name)) or must iterate with Buffer.equals.
// Result captured in docs/architecture/dlq-replay.md.

const k = Buffer.from("hello");
const m = new Map<Buffer, Buffer>();
m.set(k, Buffer.from("world"));

const direct = m.get(k);
const fresh = m.get(Buffer.from("hello"));

let iterationHit: string | null = null;
for (const [mk, mv] of m) {
  if (mk.equals(Buffer.from("hello"))) {
    iterationHit = mv.toString("utf-8");
    break;
  }
}

console.log({
  directHit: direct?.toString("utf-8") ?? null,
  freshKeyHit: fresh?.toString("utf-8") ?? null,
  iterationHit,
});
