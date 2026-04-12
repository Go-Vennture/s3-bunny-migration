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
