import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import ts from 'typescript';

const projectRoot = path.resolve(process.cwd());
const sharedSrcDir = path.resolve(projectRoot, '../london-kids-p1/packages/shared/src');
const sharedRequire = createRequire(path.join(sharedSrcDir, 'activity.ts'));
const zodUrl = pathToFileURL(sharedRequire.resolve('zod')).href;

export const tsCompilerOptions: ts.CompilerOptions = {
  module: ts.ModuleKind.ES2020,
  target: ts.ScriptTarget.ES2020,
  esModuleInterop: true,
};

const compiledDir = path.join(os.tmpdir(), 'raw-data-processor-shared');

let activitySchema: typeof import('../../london-kids-p1/packages/shared/src/activity.js').activitySchema;
let mapAddressToAreas: typeof import('../../london-kids-p1/packages/shared/src/areas.js').mapAddressToAreas;

export async function loadSharedSchema(): Promise<void> {
  const areasModule = await compileSharedModule('areas');
  const activityModule = await compileSharedModule('activity', [
    {
      pattern: /from\s+['"]\.\/areas['"]/g,
      replacement: "from './areas.mjs'",
    },
    {
      pattern: /from\s+['"]zod['"]/g,
      replacement: `from '${zodUrl}'`,
    },
  ]);

  mapAddressToAreas = areasModule.mapAddressToAreas;
  activitySchema = activityModule.activitySchema;
}

async function compileSharedModule(
  name: 'activity' | 'areas',
  replacements: { pattern: RegExp; replacement: string }[] = [],
) {
  await fs.mkdir(compiledDir, { recursive: true });
  const filePath = path.join(sharedSrcDir, `${name}.ts`);
  let code = await fs.readFile(filePath, 'utf-8');
  for (const replacement of replacements) {
    code = code.replace(replacement.pattern, replacement.replacement);
  }

  const { outputText } = ts.transpileModule(code, {
    compilerOptions: tsCompilerOptions,
    fileName: path.basename(filePath),
  });

  const adjustedOutput =
    name === 'activity'
      ? outputText.replace(/\.\/areas\.js/g, './areas.mjs')
      : outputText;

  const outPath = path.join(compiledDir, `${name}.mjs`);
  await fs.writeFile(outPath, adjustedOutput, 'utf-8');
  return import(pathToFileURL(outPath).href);
}

export { activitySchema, mapAddressToAreas };
