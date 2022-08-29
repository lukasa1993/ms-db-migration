import { totalist }      from 'totalist';
import { pathToFileURL } from 'url';

export async function glob(dir) {
  const output = [];
  await totalist(dir, (name, abs) => output.push({ name, abs }));
  return output.sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true })); // ~rand order
}

export function diff(prevs, nexts) {
  let i   = 0,
      ran,
      tmp,
      out = [];
  for (; i < nexts.length; i++) {
    tmp = nexts[i]; // file
    ran = prevs[i]; // <name>

    if (!ran) {
      out.push(tmp);
    } else if (tmp.name === ran.name) {

    } else {
      throw new Error(`Cannot run "${tmp.name}" after "${ran.name}" has been applied`);
    }
  }
  return out;
}

export function pluck(prevs, nexts) {
  let i   = 0,
      j   = 0,
      ran,
      tmp,
      out = [];
  outer: for (; i < prevs.length; i++) {
    ran = prevs[i]; // <name>
    for (j = i; j < nexts.length; j++) {
      tmp = nexts[j]; // file
      if (tmp.name === ran.name) {
        out.push(tmp);
        continue outer;
      }
    }
    throw new Error(`Cannot find "${ran.name}" migration file`);
  }
  return out;
}

export async function load(id) {
  try {
    let href = pathToFileURL(id).href;
    let m    = await import(href);
    return m.default || m; // interop
  } catch (e) {
    console.log(id, e);
    return import(id);
  }
}

export class MigrationError extends Error {
  constructor(err) {
    super(err.message);
    const details = err.stack.replace(/^(.*?)[\n\r]/, '');
    Error.captureStackTrace(err, MigrationError);
    this.stack += '\n' + details;
    Object.assign(this, err);
  }
}
