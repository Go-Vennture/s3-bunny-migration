import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const patches = [
  {
    file: "node_modules/ssh2/lib/protocol/constants.js",
    transform(text) {
      return text.replace(
        /try \{\n  cpuInfo = require\('cpu-features'\)\(\);\n\} catch \{\}\n\n/,
        "",
      );
    },
  },
  {
    file: "node_modules/ssh2/lib/protocol/crypto.js",
    transform(text) {
      return text.replace(
        /try \{\n  binding = require\('\.\/crypto\/build\/Release\/sshcrypto\.node'\);\n  \(\{ AESGCMCipher, ChaChaPolyCipher, GenericCipher,\n     AESGCMDecipher, ChaChaPolyDecipher, GenericDecipher \} = binding\);\n\} catch \{\}\n\n/,
        "",
      );
    },
  },
  {
    file: "node_modules/ssh2/lib/agent.js",
    transform(text) {
      return text.replace(
        /^\s*const EXEPATH = .*pagent\.exe.*;\r?\n/m,
        "  const EXEPATH = null;\n",
      );
    },
  },
  {
    file: "node_modules/ssh2/lib/protocol/crypto/poly1305.js",
    transform(text) {
      return text.replace('__dirname+"/",', '""+"/",');
    },
  },
];

let changed = 0;

for (const patch of patches) {
  const path = resolve(process.cwd(), patch.file);
  try {
    const original = await readFile(path, "utf8");
    const updated = patch.transform(original);
    if (updated !== original) {
      await writeFile(path, updated, "utf8");
      changed += 1;
    }
  } catch {
    // Ignore missing files so local installs and CI remain resilient.
  }
}

if (changed > 0) {
  console.log(`Patched ssh2 runtime files in ${changed} file(s).`);
}
