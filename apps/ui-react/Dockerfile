FROM node:20-alpine

WORKDIR /workspace

RUN corepack enable

COPY package.json pnpm-workspace.yaml ./
COPY apps/ui-react/package.json apps/ui-react/package.json
COPY packages/shared-types/package.json packages/shared-types/package.json
COPY . .

RUN pnpm install --no-frozen-lockfile

EXPOSE 3000

CMD ["pnpm", "--filter", "@dagflow/ui-react", "dev", "--host", "0.0.0.0", "--port", "3000"]
