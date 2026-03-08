#!/usr/bin/env node
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const pkgPath = resolve(__dirname, '..', 'package.json');
const versionPath = resolve(__dirname, '..', 'src', 'version.ts');

const pkg = JSON.parse(readFileSync(pkgPath, 'utf-8'));
const content = `export const AGENT_VERSION = '${pkg.version}';\n`;

let existing = '';
try {
  existing = readFileSync(versionPath, 'utf-8').trim();
} catch {
  // version.ts doesn't exist yet (fresh clone)
}

if (existing !== content.trim()) {
  writeFileSync(versionPath, content);
  console.log(`[sync-version] updated version.ts to ${pkg.version}`);
} else {
  console.log(`[sync-version] version.ts already at ${pkg.version}`);
}
