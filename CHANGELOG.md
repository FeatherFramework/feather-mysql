# Changelog

## 0.2.0

- Added real transactions: `DB.transaction(function(tx) ... end)`, with five methods on `tx`
  (`query`, `one`, `value`, `insert`, `exec`) that share one connection. Returning `true` commits;
  `false` or nothing rolls back.
- Added named placeholders (`@name`, `:name`) to the `MySQL.*` adapter and its exports, so a
  statement can take `{ id = 5 }` instead of a positional list. A parameter table that does not
  match the statement is refused before anything is sent.
- Added an optional oxmysql-compatible mode: with `provide 'oxmysql'` enabled and no other
  resource named `oxmysql` on disk, existing scripts written against oxmysql's `MySQL.*` library
  and `exports.oxmysql:*` work unchanged. See the README's "Existing scripts" section.
- Errors raised by the `MySQL.*` adapter's `.await` calls are now text (code, reason and the
  caller's stack trace), matching what scripts written for oxmysql expect. The native `DB.*`
  functions still raise structured error tables.
- A statement the server cannot prepare with a placeholder (for example `SHOW COLUMNS ... LIKE ?`)
  is retried once as text instead of failing outright.
- Added `feather_mysql_devmode`, which turns on query/transaction/SQL logging by default; any of
  those convars set explicitly still wins.
- A plain `DB.*` call made from inside a `DB.transaction` callback now prints a warning naming the
  file and line of the call, once per place.
- Connections are reset with `changeUser` before reuse, a timed-out statement is cancelled on the
  server with `KILL QUERY`, and a duplicate-key or other statement-level error no longer discards
  the connection.
- A startup self-check and a readiness probe report whether the bridge and the database are
  reachable before any resource that depends on this one starts using it.

## 0.1.0

- Initial release: `DB.query`, `DB.one`, `DB.value`, `DB.insert`, `DB.exec`, `DB.raw` over a
  pooled mysql2 connection, with structured errors and console diagnostics.
