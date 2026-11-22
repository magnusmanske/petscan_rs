# AGENTS.md

Project notes for working on PetScan (Rust port).

## Project overview

PetScan is a querying tool for Wikimedia projects. This repo is the Rust implementation of the backend; HTML/JS frontend lives under `html/`. Live site: https://petscan.wmflabs.org/. Built with axum (HTTP server in `src/webserver.rs`) and talks to MySQL (Wikimedia replica DBs + a local `query`/`started_queries` table).

Key paths:
- `src/` — Rust source
- `html/` — static frontend assets
- `config.json` (gitignored) — runtime config; template in `config.json.template`
- `openapi.yaml` — API spec

## Hard rules

- **Do not add the LLM attribution to commit messages.**
- **`git commit` is pre-authorized; `git push` is not.** When work is complete and the diff is reviewable, you may run `git add <specific files>` + `git commit` without asking — this is durable authorization. **Never `git push`, `git push --force`, or otherwise publish commits to a remote without an explicit per-action request from the user.** Same for any other remote-publishing action (PR creation, branch deletion on origin, etc.). Use sensible commit groupings for larger changes.
- **`audits/`, `CLAUDE.md`, `config.json`, `oauth.ini`, `flickr.key`, `.claude/` are all gitignored.** Don't try to git-add them; treat audit files as durable scratch.
- Always keep **code readability and long-term maintenance** in mind.
- Adhere to **SOLID and DRY principles**.
- Prefer a **parse, don't validate** approach. A `struct` should always be valid, ideally.
- Use **best practices** and **language standards** (idiomatic Rust, `cargo fmt`, `cargo clippy`).
- **Keep the code simple** and elegant.
- **Write tests** where it makes sense.
- **Aim to keep code small** where possible.
- **Warn me if you think what I ask of you is a bad idea.** Or just window dressing, with no improvement of functionality, UX, or code readability. Be honest.
- This is a web-facing product, so **keep security in mind** (SQL injection, XSS in rendered output, untrusted user input from query params, etc.).
