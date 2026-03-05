# ⬡ Nova File Manager

A fast, modern, self-hosted file manager built for Docker.  
Manage your entire host filesystem from a clean browser UI.

---

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env

# 2. Launch
docker compose up -d

# 3. Open
open http://localhost:9898
```

---

## Features

- **Browse & navigate** — grid and list views, breadcrumb navigation, search
- **Transfer engine** — parallel copy/move with progress, resume, and cancel
- **Conflict resolution** — overwrite, skip, or keep-both per file
- **Trash** — safe delete with restore; permanent delete option
- **Upload** — drag-and-drop or click to upload, survives browser close
- **Download** — files and directories (auto-zipped)
- **File preview** — images, video, audio, text, PDF
- **Rename & mkdir** — inline operations with keyboard shortcuts
- **Gotify alerts** — optional push notifications for job events
- **Health monitor** — sidecar container watches Nova and alerts on down
- **WebSocket** — real-time job progress without polling

---

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Ctrl+C` | Copy selected |
| `Ctrl+X` | Cut selected |
| `Ctrl+V` | Paste |
| `Ctrl+A` | Select all |
| `Delete` | Move to trash |
| `F5` | Refresh |
| `Backspace` | Go back |
| `Escape` | Deselect / close |

---

## Configuration

All settings are in `.env` (copy from `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST_PORT` | `9898` | Port exposed on the host |
| `LOG_LEVEL` | `info` | Verbosity: `debug` / `info` / `warn` / `error` |
| `COPY_WORKERS` | `4` | Parallel file copy threads |
| `GOTIFY_URL` | _(empty)_ | Gotify server URL |
| `GOTIFY_TOKEN` | _(empty)_ | Gotify app token |

---

## Ports

| Port | Service |
|------|---------|
| `9898` | Nova UI + API |
| `9091` | Monitor status JSON |

---

## Architecture

```
┌─────────────────────────┐     ┌─────────────────┐
│   nova-filemanager      │◄────│   nova-monitor  │
│   Node.js 20 + Express  │     │   health poller │
│   WebSocket (ws)        │     │   Gotify alerts │
│   Port 9898             │     │   Port 9090     │
└────────────┬────────────┘     └─────────────────┘
             │ volume mount
         /:/hostroot
```

---

## Security Note

Nova mounts your entire host filesystem (`/`) with `privileged: true`.  
**Run only on trusted networks.** Consider putting it behind an auth proxy (e.g. Authelia, Caddy basicauth) for any internet-facing deployment.
