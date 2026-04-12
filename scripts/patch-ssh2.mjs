import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const patches = [
  {
    file: "node_modules/ssh2/lib/protocol/constants.js",
    from: "try {\n  cpuInfo = require('cpu-features')();\n} catch {}\n\n",
    to: "",
  },
  {
    file: "node_modules/ssh2/lib/protocol/crypto.js",
    from: "try {\n  binding = require('./crypto/build/Release/sshcrypto.node');\n  ({ AESGCMCipher, ChaChaPolyCipher, GenericCipher,\n     AESGCMDecipher, ChaChaPolyDecipher, GenericDecipher } = binding);\n} catch {}\n\n",
    to: "",
  },
  {
    file: "node_modules/ssh2/lib/agent.js",
    from: "  const EXEPATH = resolve(__dirname, '..', 'util/pagent.exe');\n",
    to: "  const EXEPATH = typeof __dirname === 'string' ? resolve(__dirname, '..', 'util/pagent.exe') : null;\n",
  },
  {
    file: "node_modules/ssh2/lib/agent.js",
    from: "    const proc = this.proc = spawn(EXEPATH, [ data.length ]);\n",
    to: "    if (!EXEPATH) {\n      cb(new Error('Pageant is not available in this runtime'));\n      return;\n    }\n    const proc = this.proc = spawn(EXEPATH, [ data.length ]);\n",
  },
];

let changed = 0;

for (const patch of patches) {
  const path = resolve(process.cwd(), patch.file);
  try {
    const original = await readFile(path, "utf8");
    if (original.includes(patch.from)) {
      const updated = original.replace(patch.from, patch.to);
      await writeFile(path, updated, "utf8");
      changed += 1;
    }
  } catch {
    // Ignore missing files so local installs and CI remain resilient.
  }
}

if (changed > 0) {
  console.log(`Patched ssh2 native bindings in ${changed} file(s).`);
}
