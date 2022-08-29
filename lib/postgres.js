import postgres from 'postgres';
import {
  load,
  MigrationError,
}               from './util.js';

let SCHEMA = 'public';

export function connect({ schema, ...config }) {
  SCHEMA = schema;
  return postgres({
    onnotice: () => null,
    ...config,
    max: 1,
  });
}

export async function setup(sql) {
  await sql`CREATE schema if not exists ${sql(SCHEMA)}`;
  await sql`
      create table if not exists ${sql(SCHEMA)}.migrations
      (
          "id"         serial primary key,
          "name"       varchar(255) NOT NULL,
          "created_at" timestamp    NOT NULL
      )
  `;

  await sql`
      create table if not exists ${sql(SCHEMA)}.migrations
      (
          "id"         serial primary key,
          "name"       varchar(255) NOT NULL,
          "created_at" timestamp    NOT NULL
      )
  `;

  return sql`
      select "name"
      from ${sql(SCHEMA)}.migrations
      order by "id";
  `;
}

export function loop(client, files, method) {
  return client.begin(async sql => {
    for (const obj of files) {
      const file = await load(obj.abs);
      if (typeof file[method] === 'function') {
        await Promise.resolve(file[method](sql)).catch(err => {
          throw new MigrationError(err, obj);
        });
      }
      if (method === 'up') {
        await sql`insert into ${sql(SCHEMA)}."migrations" ("name", "created_at")
                  values (${obj.name}, now());`;
      } else if (method === 'down') {
        await sql`delete
                  from ${sql(SCHEMA)}."migrations"
                  where "name" = ${obj.name};`;
      }
    }
  });
}

export async function end(sql) {
  await sql.end();
}
