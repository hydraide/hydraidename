# HydrAIDE Name

Deterministic hashing and folder distribution utility for HydraIDE server-side data sharding.

## 🔧 Purpose

This internal library is used by [HydrAIDE Core](https://github.com/hydraide/core) and [HydrAIDE Server](https://github.com/hydraide/server) to ensure **predictable and even distribution** of namespace-based data (like swamps) across a scalable pool of logical servers.

> ⚠️ This package is **not intended for public use**.
> It exists solely to support the internal infrastructure of the HydrAIDE ecosystem.

---

## ✨ Features

* Hash-based deterministic server assignment
* Predictable nested folder structure generation
* Namespace-aware: works with structured paths like `Sanctuary/Realm/Swamp`
* Supports:

    * Arbitrary number of logical servers (e.g. 1000)
    * Multi-level folder depth
    * Max folders per level
* Guarantees even data distribution and uniqueness
* Enables horizontal scaling without breaking storage logic
* Lightweight: no external state or service required

---

## 🧠 How It Works

The package takes a structured path and converts it into:

1. A **server number** (between 0 and N-1),
2. A **nested folder path** suitable for sharded storage.

Given:

```
name := New().
  Sanctuary("Sanctuary1").
  Realm("RealmA").
  Swamp("SwampX")

path := name.GetFullHashPath("/hydra/data", totalServers, depth, maxFoldersPerLevel)
```

You get output like:

```
/hydra/data/600/ba22/703a
```

This tells you:

* Server 600 is responsible for this swamp
* Its folder path is `/600/ba22/703a`

---

## 🧱 Scaling Logic

Even if you only run **1 or 2 physical servers**, setting a high number of **logical servers** (e.g. 1000) allows you to:

* Distribute data in advance,
* Later **split** data ranges between servers (e.g. 0–499 stays on A, 500–999 moves to B),
* Route client requests correctly using `GetServerNumber()`.

This model removes the need for:

* External orchestrator logic
* Redistribution overhead during scale-out
* Central metadata services

Just compute → resolve → store.

---

## 📦 Installation

```bash
go get github.com/hydraide/name
```

But again:
This is for internal use only by HydrAIDE contributors. No external API guarantees, no support, no versioning. Use it only if you're working inside the core or server repo.
