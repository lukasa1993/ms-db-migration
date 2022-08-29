import { writeFileSync } from 'fs';
import { mkdir }         from 'mk-dirs';
import {
  join,
  resolve,
}                        from 'path';
import {
  connect,
  end,
  loop,
  setup,
}                        from './lib/postgres.js';
import {
  diff,
  glob,
  pluck,
}                        from './lib/util.js';

async function parse(opts) {
  const cwd = resolve(opts.cwd || '.');
  const dir = join(cwd, opts.dir);

  const migrations = await glob(dir);

  return { migrations };
}

export async function up(opts = {}) {
  let client, { migrations } = await parse(opts);

  try {
    // Open new conn; setup table
    client       = await connect(opts.config);
    const exists = await setup(client);

    const fresh = diff(exists, migrations);
    if (!fresh.length) {
      return [];
    } // nothing to run

    const toRun = fresh;
    await loop(client, toRun, 'up');
    return toRun.map(x => x.name);
  } finally {
    if (client) {
      await end(client);
    }
  }
}

export async function down(opts = {}) {
  let client, { migrations } = await parse(opts);

  try {
    // Open new conn; setup table
    client       = await connect(opts.config);
    const exists = await setup(client);
    if (!exists.length) {
      return [];
    } // nothing to undo

    exists.reverse();
    migrations.reverse();

    const last = exists[0];
    const idx  = migrations.findIndex(x => x.name === last.name);
    if (idx === -1) {
      throw new Error(`Unknown "${last.name}" migration`);
    }

    const toRun = pluck(opts.all ? exists : [last], migrations.slice(idx));
    await loop(client, toRun, 'down');
    return toRun.map(x => x.name);
  } finally {
    if (client) {
      await end(client);
    }
  }
}

export async function status(opts = {}) {
  let client, { migrations } = await parse(opts);

  try {
    client       = await connect(opts.config);
    const exists = await setup(client);
    return diff(exists, migrations).map(x => x.name);
  } finally {
    if (client) {
      await end(client);
    }
  }
}

export async function create(opts = {}) {
  let prefix = Date.now() / 1e3 | 0;

  let filename = prefix + '-' + opts.filename.replace(/\s+/g, '-');
  if (!/\.\w+$/.test(filename)) {
    filename += '.js';
  }
  let dir  = resolve(opts.cwd || '.', opts.dir);
  let file = join(dir, filename);

  await mkdir(dir).then(() => {
    let str = 'exports.up = async client => {\n\t// <insert magic here>\n};\n\n';
    str += 'exports.down = async client => {\n\t// just in case...\n};\n';
    writeFileSync(file, str);
  });

  return filename;
}
