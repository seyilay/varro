# Varro — ARO Intelligence Platform

> The definitive data-as-a-service platform for oil and gas Asset Retirement Obligations.

## Monorepo Structure

```
varro/app/
├── apps/
│   ├── web/          # Next.js 15 frontend (Vercel)
│   └── worker/       # BullMQ job workers (Railway)
├── packages/
│   ├── database/     # DB migrations + seed scripts
│   ├── shared-types/ # TypeScript interfaces shared across apps
│   └── config/       # Shared tsconfig, eslint, etc.
└── .github/
    └── workflows/    # CI/CD (lint, test, build)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Next.js 15 + React 19 |
| Backend | Next.js API routes + BullMQ workers |
| Database | Supabase (PostgreSQL 15) |
| Queue | BullMQ + Upstash Redis |
| Auth | Supabase Auth + JWT |
| Hosting | Vercel (web) + Railway (workers) |
| CI | GitHub Actions |

## Getting Started

```bash
# Install dependencies
npm install

# Set up environment
cp .env.example apps/web/.env.local
cp .env.example apps/worker/.env

# Run database migrations
# (connect to Supabase dashboard and run packages/database/migrations/*.sql)

# Start development servers
npm run dev
```

## Sprint 0 Checklist

- [x] Monorepo scaffold (turbo + workspaces)
- [x] Shared types package
- [x] Database schema (001_initial_schema.sql)
- [x] BSEE API client stub
- [x] BOEM API client stub
- [x] GitHub Actions CI workflow
- [ ] Supabase project config (dev + prod) — needs Seyi
- [ ] Vercel project link — needs Seyi
- [ ] Railway project setup — needs Seyi
- [ ] GitHub repo creation and push — needs Seyi
- [ ] Upstash Redis provisioning — needs Seyi
