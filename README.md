# ⬡ Nova File Manager

A fast, modern, self-hosted file manager for Docker. Manage your entire server filesystem from a clean browser UI — with background jobs, a built-in text editor, AI features, and PWA support.

---

## ✨ Features

| Category | What it does |
|----------|-------------|
| **Browse** | Grid & list views, breadcrumb nav, search, hidden-file toggle |
| **Transfer** | Parallel copy/move with live progress, resume & cancel |
| **Conflict resolution** | Per-file: overwrite / skip / keep-both |
| **Trash** | Safe delete with restore; permanent delete option |
| **Upload** | Drag-and-drop; survives browser close (background job) |
| **Download** | Single file or multi-select (auto-zipped on server) |
| **Zip / Unzip** | Create and extract archives as background jobs |
| **Editor** | Text editor for 50+ file types with dirty-state tracking |
| **AI (Ollama)** | Summarise, rename, describe images, semantic search, AI task agent |
| **Settings** | Show hidden files, date format, size unit, editor prefs, Dozzle URL |
| **PWA** | Installable, offline shell, home-screen shortcuts |
| **Notifications** | Gotify push alerts for completed jobs |
| **Logs** | Structured JSON logs (Dozzle-friendly); text mode also available |
| **Monitor** | Sidecar container watches Nova and sends alerts if it goes down |

---

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/nova-filemanager.git
cd nova-filemanager

# 2. Configure
cp .env.example .env
# Edit .env to set your port, Gotify, Ollama, etc.

# 3. Launch
docker compose up -d

# 4. Open
open http://localhost:9898
```

---

## ⚙ Configuration

All config lives in `.env` (copy from `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `HOST_PORT` | `9898` | Host port for the Nova UI |
| `MONITOR_PORT` | `9091` | Health monitor sidecar port |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `LOG_FORMAT` | `json` | `json` (Dozzle) or `text` (human-readable) |
| `COPY_WORKERS` | `4` | Parallel file copy threads |
| `GOTIFY_URL` | _(empty)_ | Gotify server URL |
| `GOTIFY_TOKEN` | _(empty)_ | Gotify application token |
| `OLLAMA_URL` | `http://host-gateway:11434` | Ollama base URL |
| `OLLAMA_TEXT_MODEL` | `llama3.2:1b` | Model for summarise / rename / tag |
| `OLLAMA_VISION_MODEL` | `moondream:latest` | Model for image description |
| `OLLAMA_EMBED_MODEL` | `nomic-embed-text:latest` | Model for semantic search |
| `OLLAMA_AGENT_MODEL` | _(same as text)_ | Model for AI task agent |

---

## ⌨ Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Ctrl+C` | Copy selected |
| `Ctrl+X` | Cut selected |
| `Ctrl+V` | Paste |
| `Ctrl+A` | Select all |
| `Delete` | Move to trash |
| `F5` | Refresh |
| `Backspace` | Go back |
| `Escape` | Deselect / close modal |
| `Ctrl+S` | Save in editor |

---

## 🤖 AI Features (requires Ollama)

Nova connects to a local [Ollama](https://ollama.com) instance. All AI runs locally — nothing is sent to the cloud.

```bash
ollama pull llama3.2:1b          # text ops (summarise, rename, tag)
ollama pull moondream:latest     # image description
ollama pull nomic-embed-text     # semantic search
```

Right-click any file or folder to access AI actions. The **AI Task** agent (right-click a folder) lets you describe what you want done in plain English — it proposes a plan you review before running.

---

## 📋 Dozzle Integration

Nova emits **structured JSON logs** by default, which [Dozzle](https://dozzle.dev) parses automatically.

1. Go to **Settings → Integrations** and enter your Dozzle URL
2. A **📋 Dozzle Logs ↗** button appears in the Jobs panel

To switch to human-readable logs: set `LOG_FORMAT=text` in `.env`.

---

## 🔒 Security

Nova mounts your **entire host filesystem** with `privileged: true`.

**Do not expose it to the internet without authentication.** It is designed for trusted home networks or VPN-only access. Consider fronting with an auth proxy: [Authelia](https://www.authelia.com), [Caddy basicauth](https://caddyserver.com/docs/caddyfile/directives/basicauth), or [Traefik ForwardAuth](https://doc.traefik.io/traefik/middlewares/http/forwardauth/).

---

## 🏗 Architecture

```
┌──────────────────────────┐     ┌──────────────────┐
│   nova-filemanager       │◄────│   nova-monitor   │
│   Node.js + Express      │     │   health poller  │
│   WebSocket live updates │     │   Gotify alerts  │
│   Port 9898              │     │   Port 9091      │
└────────────┬─────────────┘     └──────────────────┘
             │ bind mount
         /:/hostroot (read-write)
```

---

## 📦 Stack

- **Backend** — Node.js 20, Express, `ws`, `archiver`, `express-fileupload`
- **Frontend** — Vanilla JS + CSS (zero build step, zero dependencies)
- **AI** — Ollama (local, optional)
- **Notifications** — Gotify (optional)

---

## License

MIT
